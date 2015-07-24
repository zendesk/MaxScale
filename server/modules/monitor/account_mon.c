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
static void diagnostics(DCB *, void *);

static MONITOR_OBJECT MyObject = {
	startMonitor,
	stopMonitor,
        diagnostics
};

void account_monitor_free(ACCOUNT_MONITOR *);
void account_monitor_consume(ACCOUNT_MONITOR *, rd_kafka_message_t *);

int account_monitor_hash(void *);
int account_monitor_compare(void *, void *);

#define BROKER_PATH "/brokers/ids"

static char *get_kafka_brokerlist(ACCOUNT_MONITOR *);
static int wait_for_zookeeper(ACCOUNT_MONITOR *);
static int init_kafka(ACCOUNT_MONITOR *);

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

static char *get_kafka_brokerlist(ACCOUNT_MONITOR *handle) {
        struct String_vector brokerlist;

        if(zoo_get_children(handle->zookeeper, BROKER_PATH, 1, &brokerlist) != ZOK) {
                return NULL;
        }

        if(brokerlist.count == 0) {
                return NULL;
        }

        char *brokers = NULL;

        for(int i = 0; i < brokerlist.count; i++) {
                size_t len = sizeof(brokerlist.data[i]);
                char *path = malloc(sizeof("/brokers/ids/") + len + 1);

                if(path == NULL) {
                        return NULL;
                }

                sprintf(path, "/brokers/ids/%s", brokerlist.data[i]);

                int buffer_length = 1024;
                char buffer[buffer_length];

                zoo_get(handle->zookeeper, path, 0, buffer, &buffer_length, NULL);

                free(path);

                if(buffer_length > 0) {
                        buffer[buffer_length] = '\0';

                        char errbuf[1024];
                        yajl_val node = yajl_tree_parse(buffer, errbuf, sizeof(errbuf));

                        if(node == NULL) {
                                LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "failed to parse: %s\n\"%s\"", errbuf, buffer)));
                                continue;
                        }

                        const char *host_path[] = { "host", (const char *) 0 };
                        yajl_val host = yajl_tree_get(node, host_path, yajl_t_string);

                        if(host == NULL)  {
                                LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "failed to fetch host: \"%s\"", buffer)));
                                yajl_tree_free(node);
                                continue;
                        }

                        const char *port_path[] = { "port", (const char *) 0 };
                        yajl_val port = yajl_tree_get(node, port_path, yajl_t_number);

                        if(port == NULL)  {
                                LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "failed to fetch port: \"%s\"", buffer)));
                                yajl_tree_free(node);
                                continue;
                        }

                        char host_buffer[1024];
                        snprintf(host_buffer, 1024, "%s:%lld", YAJL_GET_STRING(host), YAJL_GET_INTEGER(port));

                        yajl_tree_free(node);

                        int previous_size = brokers == NULL ? 0 : strlen(brokers);
                        // strlen + , + strlen
                        int len = previous_size + strlen(host_buffer);

                        if(i > 0) {
                                len++;
                        }

                        char *new_brokers = malloc(len);

                        if(new_brokers == NULL) {
                                continue;
                        }

                        if(brokers != NULL) {
                                memcpy(new_brokers, brokers, previous_size);
                                free(brokers);
                        }

                        if(i > 0) {
                                new_brokers[previous_size] = ',';
                                previous_size++;
                        }

                        memcpy(new_brokers + previous_size, host_buffer, strlen(host_buffer));

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
                char *brokers = get_kafka_brokerlist(handle);

                if(brokers != NULL) {
                        rd_kafka_brokers_add(handle->connection, brokers);
                }
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

                        if(handle->topic_name == NULL) {
                                account_monitor_free(handle);
                                return NULL;
                        }

                        strcpy(handle->topic_name, params->value);
                }

                params = params->next;
        }

        if(zookeeper == NULL) {
                LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "missing required parameter: zookeeper")));
                account_monitor_free(handle);
                return NULL;
        }

        if(handle->topic_name == NULL) {
                LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "missing required parameter: topic")));
                account_monitor_free(handle);
                return NULL;
        }

        handle->zookeeper = zookeeper_init(zookeeper, watcher, 10000, 0, (void *) handle, 0);

        if(handle->zookeeper == NULL) {
                account_monitor_free(handle);
                return NULL;
        }

        handle->configuration = rd_kafka_conf_new();

        handle->shutdown = 0;

        handle->accounts = hashtable_alloc(10000, account_monitor_hash, account_monitor_compare);

        if(handle->accounts == NULL) {
                account_monitor_free(handle);
                return NULL;
        }

        handle->tid = (THREAD) thread_start(monitorMain, handle);

        return handle;
}

static void stopMonitor(void *arg) {
        MONITOR *monitor = (MONITOR *) arg;
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) monitor->handle;

        if(handle != NULL) {
                handle->shutdown = 1;
                thread_wait((void *) handle->tid);

                account_monitor_free(handle);
                monitor->handle = NULL;
        }
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

        dcb_printf(dcb, "\tMaxScale MonitorId:\t%lu\n", handle->id);

        if(handle->accounts != NULL) {
                int hashsize, total, longest;
                hashtable_get_stats(handle->accounts, &hashsize, &total, &longest);

                dcb_printf(dcb, "\tAccounts hashsize:\t%i\n", hashsize);
                dcb_printf(dcb,"\tAccounts total:\t\t%i\n", total);
                dcb_printf(dcb,"\tAcconts longest chain:\t\t%i\n", longest);
        }
}

static void logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
        LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "rdkafka: %s", buf)));
}

static int wait_for_zookeeper(ACCOUNT_MONITOR *handle) {
        int nrounds = 0;

        while(!handle->connected) {
                if(nrounds > 100) {
                        return 1;
                }

                thread_millisleep(1000);
                nrounds++;
        }

        return 0;
}

static int init_kafka(ACCOUNT_MONITOR *handle) {
        char errbuf[1024];

        handle->connection = rd_kafka_new(RD_KAFKA_CONSUMER, handle->configuration, errbuf, sizeof(errbuf));

        if(handle->connection == NULL) {
                LOGIF(LM, (skygw_log_write(LOGFILE_ERROR, "Could not create kafka connection.\n%s", errbuf)));
                return 1;
        }

        char *brokers = get_kafka_brokerlist(handle);

        if(brokers != NULL) {
                rd_kafka_brokers_add(handle->connection, brokers);
        }

        rd_kafka_topic_conf_t *topic_configuration = rd_kafka_topic_conf_new();
        handle->topic = rd_kafka_topic_new(handle->connection, handle->topic_name, topic_configuration);

        if(handle->topic == NULL) {
                LOGIF(LM, (skygw_log_write(LOGFILE_ERROR, "Could not create kafka topic.\n%s", rd_kafka_err2str(rd_kafka_errno2err(errno)))));
                return 1;
        }

        rd_kafka_set_logger(handle->connection, logger);

        if(rd_kafka_metadata(handle->connection, 0, handle->topic, &handle->metadata, 5000) != RD_KAFKA_RESP_ERR_NO_ERROR) {
                LOGIF(LM, (skygw_log_write(LOGFILE_ERROR, "Could not fetch topic metadata (partitions).\n%s", rd_kafka_err2str(rd_kafka_errno2err(errno)))));
                return 1;
        }

        handle->queue = rd_kafka_queue_new(handle->connection);

        for(int i = 0; i < handle->metadata->topics[0].partition_cnt; i++) {
                int partition = handle->metadata->topics[0].partitions[i].id;

                if(rd_kafka_consume_start_queue(handle->topic, partition, RD_KAFKA_OFFSET_BEGINNING, handle->queue) == -1) {
                        LOGIF(LM, (skygw_log_write(LOGFILE_ERROR, "Failed to start consuming partition %i: %s", partition, rd_kafka_err2str(rd_kafka_errno2err(errno)))));
                        return 1;
                }
        }

        return 0;
}

static void monitorMain(void *arg) {
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) arg;
        handle->status = MONITOR_RUNNING;

        if(wait_for_zookeeper(handle) != 0) {
                LOGIF(LM, (skygw_log_write(LOGFILE_ERROR, "Could not obtain zookeeper connection.", version_str)));
                account_monitor_free(handle);
                return;
        }

        if(init_kafka(handle) != 0) {
                account_monitor_free(handle);
                return;
        }

        while(handle->shutdown == 0) {
                rd_kafka_message_t *message = rd_kafka_consume_queue(handle->queue, 1000);

                if(message == NULL) {
                        continue;
                }

                account_monitor_consume(handle, message);
                rd_kafka_message_destroy(message);
        }
}

void account_monitor_consume(ACCOUNT_MONITOR *handle, rd_kafka_message_t *message) {
        if(message->len == 0) {
                return;
        }

        char errbuf[1024];
        char *payload = message->payload;

        // sort of a right-stripping type mechanism
        // we know json ends with a '}', so just dump everything else
        int i;
        for(i = message->len; i >= 0; i--) {
                if(payload[i] != '}') {
                        payload[i] = '\0';

                        // We got this far and there's nothing in the string
                        if(i == 0) {
                                return;
                        }
                }
        }

        yajl_val node = yajl_tree_parse(payload, errbuf, sizeof(errbuf));

        if(node == NULL) {
                LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "failed to parse: %s\n\"%s\"", errbuf, message->payload)));
                return;
        }

        const char *table_path[] = { "table", (const char *) 0 };
        yajl_val table_node = yajl_tree_get(node, table_path, yajl_t_string);

        if(table_node == NULL) {
                return;
        }

        char *table = YAJL_GET_STRING(table_node);

        if(table == NULL || strcmp(table, "accounts") != 0) {
                yajl_tree_free(node);
                return;
        }

        const char *id_path[] = { "data", "id", (const char *) 0 };
        yajl_val id_node = yajl_tree_get(node, id_path, yajl_t_number);

        if(id_node == NULL) {
                yajl_tree_free(node);
                return;
        }

        const char *shard_id_path[] = { "data", "shard_id", (const char *) 0 };
        yajl_val shard_id_node = yajl_tree_get(node, shard_id_path, yajl_t_number);

        if(shard_id_node == NULL) {
                yajl_tree_free(node);
                return;
        }

        long long int id = YAJL_GET_INTEGER(id_node);
        long long int shard_id = YAJL_GET_INTEGER(shard_id_node);

        hashtable_delete(handle->accounts, (void *) id);
        hashtable_add(handle->accounts, (void *) id, (void *) shard_id);

        LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "found shard_id %lld for account %lld", shard_id, id)));

        yajl_tree_free(node);
}

void account_monitor_free(ACCOUNT_MONITOR *handle) {
        handle->status = MONITOR_STOPPED;

        if(handle->queue != NULL) {
                rd_kafka_queue_destroy(handle->queue);
                handle->queue = NULL;
        }

        if(handle->topic != NULL) {
                if(handle->metadata != NULL) {
                        for(int i = 0; i < handle->metadata->topics[0].partition_cnt; i++) {
                                int partition = handle->metadata->topics[0].partitions[i].id;
                                rd_kafka_consume_stop(handle->topic, partition);
                        }

                        rd_kafka_metadata_destroy(handle->metadata);
                        handle->metadata = NULL;
                }


                rd_kafka_topic_destroy(handle->topic);
                handle->topic = NULL;
        }

        if(handle->connection != NULL) {
                rd_kafka_destroy(handle->connection);
                handle->connection = NULL;
        }

        if(handle->zookeeper != NULL) {
                zookeeper_close(handle->zookeeper);
                handle->zookeeper = NULL;
        }

        if(handle->accounts != NULL) {
                hashtable_free(handle->accounts);
                handle->accounts = NULL;
        }

        if(handle->topic_name != NULL) {
                free(handle->topic_name);
        }

        free(handle);
}

int account_monitor_hash(void *key) {
        if(key == NULL)
                return 0;

        return ((long long int) key) % 10000;
}

int account_monitor_compare(void *v1, void *v2) {
  if(v1 == v2) {
          return 0;
  } else {
          return 1;
  }
}
