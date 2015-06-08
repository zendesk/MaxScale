/* vim: set ts=8 sw=8 noexpandtab */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <microhttpd.h>

#include "dcb.h"
#include "flexmaster_monitor.h"
#include "log_manager.h"
#include "monitor.h"

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread log_info_t tls_log_info;

static void monitorMain(void *);

static char *version_str = "V0.0.0";

MODULE_INFO info = {
        MODULE_API_MONITOR,
        MODULE_GA,
        MONITOR_VERSION,
        "An httpd server for initiating master-slave swaps"
};

static void *startMonitor(void *,void*);
static void stopMonitor(void *);
static void diagnostics(DCB *, void *);

static MONITOR_OBJECT MyObject = {
	startMonitor,
	stopMonitor,
        diagnostics
};

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
        LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "Initialise the flexmaster monitor module %s.", version_str)));
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

                if (nrounds != 0 && ((nrounds * MON_BASE_INTERVAL_MS) % handle->interval) >= MON_BASE_INTERVAL_MS) {
                        nrounds += 1;
			continue;
		}

                nrounds += 1;
        }
}
