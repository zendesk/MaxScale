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

        while(params) {
                /*
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
                */

                params = params->next;
        }



        handle->shutdown = 0;
        handle->tid = (THREAD) thread_start(monitorMain, handle);

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

static void monitorMain(void *arg) {
        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) arg;

        handle->status = MONITOR_RUNNING;
        size_t nrounds = 0;

        while(1) {
                if(handle->shutdown) {
                        handle->status = MONITOR_STOPPING;
                        handle->status = MONITOR_STOPPED;
                        return;
                }

                thread_millisleep(MON_BASE_INTERVAL_MS);

                if (nrounds != 0 && ((nrounds * MON_BASE_INTERVAL_MS) % handle->interval) >= MON_BASE_INTERVAL_MS) {
                        nrounds += 1;
			continue;
		}

                nrounds += 1;

                if(handle->accounts == NULL) {
                        handle->accounts = hashtable_alloc(10000, account_monitor_hash, account_monitor_compare);
                }
        }
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
