/* vim: set ts=8 sw=8 noexpandtab */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <monitor.h>
#include <account_mon.h>
#include <thread.h>
#include <mysql.h>
#include <mysqld_error.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <secrets.h>
#include <dcb.h>
#include <modinfo.h>
#include <maxconfig.h>
#include <yajl/yajl_tree.h>

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread log_info_t tls_log_info;

static void monitorMain(void *);

static char *version_str = "V1.0.0";

MODULE_INFO info = {
        MODULE_API_MONITOR,
        MODULE_GA,
        MONITOR_VERSION,
        "A Zendesk-specific account shard monitor"
};

static void *startMonitor(void *,void*);
static void stopMonitor(void *);
static void registerServer(void *, SERVER *);
static void unregisterServer(void *, SERVER *);
static void defaultUser(void *, char *, char *);
static void diagnostics(DCB *, void *);
static void setInterval(void *, size_t);
static void setNetworkTimeout(void *, int, int);

static MONITOR_OBJECT MyObject = {
	startMonitor,
	stopMonitor,
        diagnostics
};

int account_monitor_hash(void *);
int account_monitor_compare(void *, void *);

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *version() {
        return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void ModuleInit() {
        LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "Initialise the Account Monitor module %s.", version_str)));
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
MONITOR_OBJECT *GetModuleObject() {
	return &MyObject;
}

#define BROKER_PATH "/brokers/ids"

static char *set_kafka_brokerlist(ACCOUNT_MONITOR *handle) {
        struct String_vector brokerlist;

        if(zoo_get_children(handle->zookeeper, BROKER_PATH, 1, &brokerlist) != ZOK) {
                // TODO
                return NULL;
        }

        if(brokerlist.count == 0) {
                // TODO
                return NULL;
        }

        char *brokers = NULL;

        for(int i = 0; i < brokerlist.count; i++) {
                size_t len = sizeof(brokerlist.data[i]);
                char *path = malloc(sizeof("/brokers/ids/") + len + 1);
                sprintf(path, "/brokers/ids/%s", brokerlist.data[i]);
                //if(path == NULL)

                int buffer_length = 1024;
                char buffer[buffer_length];

                zoo_get(handle->zookeeper, path, 0, buffer, &buffer_length, NULL);

                if(buffer_length > 0) {
                        buffer[buffer_length] = '\0';

                        char errbuf[1024];
                        yajl_val node = yajl_tree_parse(buffer, errbuf, sizeof(errbuf));

                        if(node == NULL) {
                                // TODO
                                continue;
                        }

                        const char *host_path[] = { "host" };
                        yajl_val host = yajl_tree_get(node, host_path, yajl_t_string);

                        const char *port_path[] = { "port" };
                        yajl_val port = yajl_tree_get(node, port_path, yajl_t_number);

                        char host_buffer[1024];
                        snprintf(host_buffer, 1024, "%s:%lld", YAJL_GET_STRING(host), YAJL_GET_INTEGER(port));

                        int previous_size = brokers == NULL ? 0 : strlen(brokers);
                        // strlen + , + strlen + \0
                        int len = previous_size + strlen(host_buffer) + 2;
                        char *new_brokers = malloc(len);

                        memcpy(new_brokers, brokers, previous_size);
                        free(brokers);

                        new_brokers[previous_size] = ',';

                        memcpy(new_brokers + previous_size + 1, host_buffer, strlen(host_buffer));

                        new_brokers[len] = '\0';
                        brokers = new_brokers;
                }
        }

        deallocate_String_vector(&brokerlist);

        return brokers;
}

static void watcher(zhandle_t *zookeeper, int type, int state, const char *path, void *context) {
        ACCOUNT_MONITOR *handle = context;

        if (type == ZOO_SESSION_EVENT && state == ZOO_CONNECTED_STATE) {
                handle->connected = true;
        } else if(type == ZOO_CHILD_EVENT && strncmp(path, BROKER_PATH, sizeof(BROKER_PATH) - 1) == 0) {
                char *brokers = set_kafka_brokerlist(handle);
                rd_kafka_brokers_add(handle->connection, brokers);
        }
}

static void *startMonitor(void *arg, void *opt) {
        MONITOR *monitor = (MONITOR *) arg;
        ACCOUNT_MONITOR *handle = monitor->handle;

        if(handle == NULL) {
                handle = (ACCOUNT_MONITOR *) calloc(1, sizeof(ACCOUNT_MONITOR));

                if(handle == NULL)
                        return NULL;

                handle->id = config_get_gateway_id();
                handle->interval = MONITOR_INTERVAL;
                handle->accounts = NULL;
                spinlock_init(&handle->lock);
        }

        CONFIG_PARAMETER *params = opt;
        char *zookeeper = NULL;

        while(params) {
                if(strcasecmp(params->name, "zookeeper") == 0) {
                        zookeeper = params->value;
                }

                if(strcasecmp(params->name, "topic") == 0) {
                        handle->topic_name = malloc(sizeof(params->value));
                        // if topic_name == NULL
                        strcpy(handle->topic_name, params->value);
                }

                params = params->next;
        }

        // if zookeeper == NULL || topic == NULL

        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        handle->zookeeper = zookeeper_init(zookeeper, watcher, 10000, 0, (void *) handle, 0);

        if(handle->zookeeper == NULL) {
                // TODO error
                free(handle);
                return NULL;
        }

        handle->configuration = rd_kafka_conf_new();

        handle->shutdown = 0;
        handle->tid = (THREAD) thread_start(monitorMain, handle);

        handle->accounts = hashtable_alloc(10000, account_monitor_hash, account_monitor_compare);
        // check err

        return handle;
}

static void stopMonitor(void *arg) {
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) arg;

        handle->shutdown = 1;
        thread_wait((void *) handle->tid);
}

static void registerServer(void *arg, SERVER *server) {
        // unused
}

static void unregisterServer(void *arg, SERVER *server) {
        // unused
}

static void defaultUser(void *arg, char *user, char *password) {
        // unused
}

static void setNetworkTimeout(void *arg, int type, int value) {
        // unused
}

static void diagnostics(DCB *dcb, void *arg) {
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) arg;

        switch (handle->status) {
                case MONITOR_RUNNING:
                        dcb_printf(dcb, "\tMonitor running\n");
                        break;
                case MONITOR_STOPPING:
                        dcb_printf(dcb, "\tMonitor stopping\n");
                        break;
                case MONITOR_STOPPED:
                        dcb_printf(dcb, "\tMonitor stopped\n");
                        break;
        }

        dcb_printf(dcb, "\tSampling interval:\t%lu milliseconds\n", handle->interval);
        dcb_printf(dcb, "\tMaxScale MonitorId:\t%lu\n", handle->id);

        int hashsize, total, longest;
        hashtable_get_stats(handle->accounts, &hashsize, &total, &longest);

        dcb_printf(dcb, "\tAccounts hashsize:\t%i\n", hashsize);
        dcb_printf(dcb,"\tAccounts total:\t\t%i\n", total);
        dcb_printf(dcb,"\tAcconts longest chain:\t\t%i\n", longest);
}

static void setInterval(void *arg, size_t interval) {
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) arg;
        memcpy(&handle->interval, &interval, sizeof(unsigned long));
}

static void logger(const rd_kafka_t *rk, int level,
                const char *fac, const char *buf) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
                        (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
                        level, fac, rd_kafka_name(rk), buf);
}


static void monitorMain(void *arg) {
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) arg;

        handle->status = MONITOR_RUNNING;
        size_t nrounds = 0;

        while(!handle->connected) {
                sleep(1);
                // help
        }

        // check err
        char *brokers = set_kafka_brokerlist(handle);

        char errbufa[1024];
        printf("%s\n", brokers);
        if(rd_kafka_conf_set(handle->configuration, "metadata.broker.list", brokers, errbufa, sizeof(errbufa) != RD_KAFKA_CONF_OK)) {
                // ERR TODO
        }


        char errbufb[1024];
        handle->connection = rd_kafka_new(RD_KAFKA_CONSUMER, handle->configuration, errbufb, sizeof(errbufb));

        if(handle->connection != 0) {
                // TODO
        }

        rd_kafka_topic_conf_t *topic_configuration = rd_kafka_topic_conf_new();
        handle->topic = rd_kafka_topic_new(handle->connection, handle->topic_name, topic_configuration);

        // check err?

        rd_kafka_set_logger(handle->connection, logger);
        rd_kafka_set_log_level(handle->connection, 7);

        if(rd_kafka_consume_start(handle->topic, 0, RD_KAFKA_OFFSET_BEGINNING) == -1){
                fprintf(stderr, "%% Failed to start consuming: %s\n", rd_kafka_err2str(rd_kafka_errno2err(errno)));
        }

        while(1) {
                if(handle->shutdown) {
                        handle->status = MONITOR_STOPPING;
                        handle->status = MONITOR_STOPPED;
                        break;
                }

                /*thread_millisleep(MON_BASE_INTERVAL_MS);

                if (nrounds != 0 && ((nrounds * MON_BASE_INTERVAL_MS) % handle->interval) >= MON_BASE_INTERVAL_MS) {
                        nrounds += 1;
			continue;
		}

                nrounds += 1;*/

                rd_kafka_message_t *message = rd_kafka_consume(handle->topic, 0, 1000);
                if(message == NULL)
                        continue;

                // msg_consume(rkmessage, NULL);

                printf("%.*s\n", (int) message->len, (char *) message->payload);
                rd_kafka_message_destroy(message);
        }

        rd_kafka_consume_stop(handle->topic, 0);

        //TODO
        //rd_kafka_topic_destroy(rkt);
        //rd_kafka_destroy(rk);
}

int account_monitor_hash(void *key) {
        if(key == NULL)
                return 0;

        return *((int *) key) % 10000;
}

int account_monitor_compare(void *v1, void *v2) {
  int *i1 = (int *) v1;
  int *i2 = (int *) v2;

  if(*i1 == *i2) {
          return 0;
  } else {
          return 1;
  }
}
