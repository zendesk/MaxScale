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
	registerServer,
	unregisterServer,
	defaultUser,
	diagnostics,
	setInterval,
	setNetworkTimeout
};

int account_monitor_connect(MYSQL_MONITOR *);
int account_monitor_close(MYSQL_MONITOR *);

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
        MYSQL_MONITOR *handle;

        if(arg) { /* Must be a restart */
                handle = arg;
        } else {
                handle = (MYSQL_MONITOR *) malloc(sizeof(MYSQL_MONITOR));

                if(handle == NULL)
                        return NULL;

                SERVICE *service = service_find("top service");

                if(service == NULL)
                        return NULL;

                handle->service = service;
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
        thread_wait((void *)handle->tid);
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

	dcb_printf(dcb,"\tSampling interval:\t%lu milliseconds\n", handle->interval);
	dcb_printf(dcb,"\tMaxScale MonitorId:\t%lu\n", handle->id);
	dcb_printf(dcb,"\tConnect Timeout:\t%i seconds\n", handle->connect_timeout);
	dcb_printf(dcb,"\tRead Timeout:\t\t%i seconds\n", handle->read_timeout);
	dcb_printf(dcb,"\tWrite Timeout:\t\t%i seconds\n", handle->write_timeout);

        // TODO: Print account stats
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
        int retries;

        for(retries = 0; retries < 10; retries ++) {
                if(account_monitor_connect(handle) == 0) {
                        break;
                } else {
                        thread_millisleep(MON_BASE_INTERVAL_MS);
                }
        }

        if(handle->connection == NULL) {
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Fatal: account monitor could not initiate a connection\n")));

                handle->status = MONITOR_STOPPING;
                mysql_thread_end();
                handle->status = MONITOR_STOPPED;
                return;
        }

        size_t nrounds = 0;

        while(1) {
                if(handle->shutdown) {
                        handle->status = MONITOR_STOPPING;
                        mysql_thread_end();
                        handle->status = MONITOR_STOPPED;
                        return;
                }

                thread_millisleep(MON_BASE_INTERVAL_MS);

                if (nrounds != 0 && ((nrounds * MON_BASE_INTERVAL_MS) % handle->interval) >= MON_BASE_INTERVAL_MS) {
			nrounds += 1;
			continue;
		}

		nrounds += 1;

                if(mysql_query(handle->connection, "SELECT id, shard_id FROM accounts") != 0) {
                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not query accounts")));
                        // TODO?
                }

                MYSQL_RES *result = mysql_store_result(handle->connection);

                if(result == NULL) {
                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not fetch results")));
                }

                MYSQL_ROW row;

                int numAccounts = 0;

                while((row = mysql_fetch_row(result)) != NULL) {
                        handle->accounts = realloc(handle->accounts, (numAccounts + 2) * sizeof(int *));

                        int *account = malloc(sizeof(int) * 2);
                        account[0] = strtol(row[0], NULL, 0);
                        account[1] = strtol(row[1], NULL, 0);

                        handle->accounts[numAccounts++] = account;
                }

                skygw_log_write(LOGFILE_TRACE, "found %d accounts", numAccounts - 1);
                handle->accounts[numAccounts] = NULL;

                mysql_free_result(result);
        }
}

int account_monitor_connect(MYSQL_MONITOR *handle) {
        if(handle->service->dbref == NULL)
                return 1;

        // just choose one server
        SERVER *server = handle->service->dbref->server;

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
                LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "Error: could not connect to server")));
                return account_monitor_close(handle);
        }

        return 0;
}

int account_monitor_close(MYSQL_MONITOR *handle) {
        mysql_close(handle->connection);
        handle->connection = NULL;
        return 1;
}
