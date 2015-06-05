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

int account_monitor_connect(MYSQL_MONITOR *);
int account_monitor_close(MYSQL_MONITOR *);
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

static void *startMonitor(void *arg, void *opt) {
        MONITOR *monitor = (MONITOR *) arg;
        MYSQL_MONITOR *handle = monitor->handle;

        if(handle == NULL) {
                handle = (MYSQL_MONITOR *) calloc(1, sizeof(MYSQL_MONITOR));

                if(handle == NULL)
                        return NULL;

                handle->id = config_get_gateway_id();
                handle->interval = MONITOR_INTERVAL;
                handle->connect_timeout = DEFAULT_CONNECT_TIMEOUT;
                handle->read_timeout = DEFAULT_READ_TIMEOUT;
                handle->write_timeout = DEFAULT_WRITE_TIMEOUT;
                handle->connection = NULL;
                handle->accounts = NULL;
                spinlock_init(&handle->lock);
        }

        CONFIG_PARAMETER* params = (CONFIG_PARAMETER *) opt;

        while(params) {
                if(strcasecmp(params->name, "account_database") == 0) {
                        handle->account_database = strdup(params->value);
                }

                if(strcasecmp(params->name, "account_service") == 0) {
                        handle->service = service_find(params->value);

                        if(handle->service == NULL) {
                                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "account monitor: could not find service %s for accounts", params->value)));
                                // TODO: fully error?
                        }
                }

                params = params->next;
        }

        if(handle->account_database == NULL) {
                // default account db of zd_account
                handle->account_database = calloc(11, sizeof(char));
                strcpy(handle->account_database, "zd_account");
        }

        handle->shutdown = 0;
        handle->tid = (THREAD) thread_start(monitorMain, handle);

        return handle;
}

static void stopMonitor(void *arg) {
        MYSQL_MONITOR *handle = (MYSQL_MONITOR *) arg;

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

static void diagnostics(DCB *dcb, void *arg) {
        MYSQL_MONITOR *handle = (MYSQL_MONITOR *) arg;

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
        dcb_printf(dcb, "\tConnect Timeout:\t%i seconds\n", handle->connect_timeout);
        dcb_printf(dcb, "\tRead Timeout:\t\t%i seconds\n", handle->read_timeout);
        dcb_printf(dcb, "\tWrite Timeout:\t\t%i seconds\n", handle->write_timeout);

        int hashsize, total, longest;
        hashtable_get_stats(handle->accounts, &hashsize, &total, &longest);

        dcb_printf(dcb, "\tAccounts hashsize:\t%i\n", hashsize);
        dcb_printf(dcb,"\tAccounts total:\t\t%i\n", total);
        dcb_printf(dcb,"\tAcconts longest chain:\t\t%i\n", longest);
}

static void setInterval(void *arg, size_t interval) {
        MYSQL_MONITOR *handle = (MYSQL_MONITOR *) arg;
        memcpy(&handle->interval, &interval, sizeof(unsigned long));
}

static void setNetworkTimeout(void *arg, int type, int value) {
        MYSQL_MONITOR *handle = (MYSQL_MONITOR *) arg;
        int max_timeout = (int) (handle->interval / 1000);
        int new_timeout = max_timeout -1;

        if (new_timeout <= 0)
                new_timeout = DEFAULT_CONNECT_TIMEOUT;

        switch(type) {
                case MONITOR_CONNECT_TIMEOUT:
                        if (value < max_timeout) {
                                memcpy(&handle->connect_timeout, &value, sizeof(int));
                        } else {
                                memcpy(&handle->connect_timeout, &new_timeout, sizeof(int));
                                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "warning : Monitor Connect Timeout %i is greater than monitor interval ~%i seconds, lowering to %i seconds", value, max_timeout, new_timeout)));
                        }

                        break;
                case MONITOR_READ_TIMEOUT:
                        if (value < max_timeout) {
                                memcpy(&handle->read_timeout, &value, sizeof(int));
                        } else {
                                memcpy(&handle->read_timeout, &new_timeout, sizeof(int));
                                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "warning : Monitor Read Timeout %i is greater than monitor interval ~%i seconds, lowering to %i seconds", value, max_timeout, new_timeout)));
                        }

                        break;
                case MONITOR_WRITE_TIMEOUT:
                        if (value < max_timeout) {
                                memcpy(&handle->write_timeout, &value, sizeof(int));
                        } else {
                                memcpy(&handle->write_timeout, &new_timeout, sizeof(int));
                                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "warning : Monitor Write Timeout %i is greater than monitor interval ~%i seconds, lowering to %i seconds", value, max_timeout, new_timeout)));
                        }

                        break;
                default:
                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error : Monitor setNetworkTimeout received an unsupported action type %i", type)));
        }
}


static void monitorMain(void *arg) {
        MYSQL_MONITOR *handle = (MYSQL_MONITOR *) arg;

        if(mysql_thread_init() != 0) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Fatal: mysql_thread_init failed in monitor module. Exiting.\n")));
                return;
        }

        handle->status = MONITOR_RUNNING;
        size_t nrounds = 0;

        while(1) {
                if(handle->shutdown) {
                        handle->status = MONITOR_STOPPING;
                        mysql_thread_end();
                        handle->status = MONITOR_STOPPED;
                        return;
                }

                thread_millisleep(MON_BASE_INTERVAL_MS);

                if(handle->current_server == NULL || !(SERVER_IS_RUNNING(handle->current_server) && SERVER_IS_SLAVE(handle->current_server))) {
                        if(handle->connection != NULL) {
                                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: current server is not slave nor running")));
                                account_monitor_close(handle);
                        }

                        account_monitor_connect(handle);
                }

                if(handle->connection == NULL) {
                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not query accounts -- no connection")));
                        continue;
                }

                if (nrounds != 0 && ((nrounds * MON_BASE_INTERVAL_MS) % handle->interval) >= MON_BASE_INTERVAL_MS) {
                        nrounds += 1;
			continue;
		}

                nrounds += 1;

                if(mysql_query(handle->connection, "SELECT id, shard_id FROM accounts") != 0) {
                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not query accounts")));
                        continue;
                }

                MYSQL_RES *result = mysql_store_result(handle->connection);

                if(result == NULL) {
                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not fetch results")));
                        continue;
                }

                MYSQL_ROW row;

                if(handle->accounts == NULL) {
                        handle->accounts = hashtable_alloc(10000, account_monitor_hash, account_monitor_compare);
                }

                int *key, *value;

                while((row = mysql_fetch_row(result)) != NULL) {
                        key = malloc(sizeof(int));
                        value = malloc(sizeof(int));

                        *key = strtol(row[0], NULL, 0);
                        *value = strtol(row[1], NULL, 0);

                        hashtable_add(handle->accounts, key, value);
                }

                int numAccounts = mysql_num_rows(result);
                skygw_log_write(LOGFILE_MESSAGE, "account monitor: found %d accounts", numAccounts);

                mysql_free_result(result);
        }
}

SERVER *account_monitor_find_slave(SERVER_REF *dbref) {
        SERVER *server;

        while(dbref != NULL) {
                server = dbref->server;

                if(SERVER_IS_RUNNING(server) && SERVER_IS_SLAVE(server))
                        return server;

                dbref = dbref->next;
        }

        return NULL;
}

int account_monitor_connect(MYSQL_MONITOR *handle) {
        if(handle->service->dbref == NULL)
                return 1;

        SERVER_REF *dbref = handle->service->dbref;
        SERVER *server = account_monitor_find_slave(dbref);

        if(server == NULL)
                return 1;

        handle->connection = mysql_init(NULL);

        if(handle->connection == NULL) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not initialize mysql connection")));
                return 1;
        }

	if(mysql_options(handle->connection, MYSQL_OPT_READ_TIMEOUT, (void *) &handle->read_timeout) != 0) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: failed to set read timeout value for backend connection.")));
                return account_monitor_close(handle);
	}
	
	if(mysql_options(handle->connection, MYSQL_OPT_CONNECT_TIMEOUT, (void *) &handle->connect_timeout) != 0) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: failed to set connect timeout value for backend connection.")));
                return account_monitor_close(handle);
	}
	
	if(mysql_options(handle->connection, MYSQL_OPT_WRITE_TIMEOUT, (void *) &handle->write_timeout) != 0) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: failed to set connect timeout value for backend connection.")));
                return account_monitor_close(handle);
	}

        if(mysql_options(handle->connection, MYSQL_OPT_USE_REMOTE_CONNECTION, NULL) != 0) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: failed to set external connection. It is needed for backend server connections.")));
                return account_monitor_close(handle);
        }

        if(mysql_real_connect(handle->connection, server->name, handle->service->credentials.name, handle->service->credentials.authdata, handle->account_database, server->port, NULL, 0) == NULL) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not connect to server %s:%d as %s", server->name, server->port, handle->service->credentials.name)));
                return account_monitor_close(handle);
        }

        skygw_log_write(LOGFILE_MESSAGE, "account monitor: connected to %s", server->name);
        handle->current_server = server;

        return 0;
}

int account_monitor_close(MYSQL_MONITOR *handle) {
        mysql_close(handle->connection);
        handle->connection = NULL;
        handle->current_server = NULL;

        return 1;
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
