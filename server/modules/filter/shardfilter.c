/* vim: set ts=8 sw=8 noexpandtab */

#include "account_mon.h"
#include "buffer.h"
#include "filter.h"
#include "log_manager.h"
#include "modutil.h"
#include "monitor.h"
#include "mysql_client_server_protocol.h"

#include <mysql.h>
#include <stdio.h>
#include <time.h>

/**
 * @file shardfilter.c - a shard selection filter
 * @verbatim
 *
 * This filter is a very simple example used to test the filter API,
 * it merely counts the number of statements that flow through the
 * filter pipeline.
 *
 * Reporting is done via the diagnostics print routine.
 * @endverbatim
 */

MODULE_INFO info = {
        MODULE_API_FILTER,
        MODULE_BETA_RELEASE,
        FILTER_VERSION,
        "Zendesk's custom shard switching filter"
};

static char *version_str = "V1.0.0";

static FILTER *createInstance(char **, FILTER_PARAMETER **);
static void *newSession(FILTER *, SESSION *);
static void closeSession(FILTER *, void *);
static void freeSession(FILTER *, void *);
static void setDownstream(FILTER *, void *, DOWNSTREAM *);
static int routeQuery(FILTER *, void *, GWBUF *);
static void diagnostic(FILTER *, void *, DCB *);

static FILTER_OBJECT MyObject = {
        createInstance,
        newSession,
        closeSession,
        freeSession,
        setDownstream,
        NULL,		// No upstream requirement
        routeQuery,
        NULL,
        diagnostic,
};

typedef struct {
        int sessions;
        SERVICE **downstreams;

        char *shard_format;

        MONITOR *account_monitor;
        SPINLOCK lock;
} ZENDESK_INSTANCE;

typedef struct {
        SESSION *rses;

        DOWNSTREAM shard_server;

        int shard_id;

        SPINLOCK lock;
} ZENDESK_SESSION;

SERVICE *shardfilter_service_for_shard(ZENDESK_INSTANCE *, char *);
int shardfilter_find_shard(ZENDESK_INSTANCE *, int);
int shardfilter_find_account(char *, int);

extern int lm_enabled_logfiles_bitmask;
extern size_t log_ses_count[];
extern __thread log_info_t tls_log_info;

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
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
FILTER_OBJECT *GetModuleObject() {
        return &MyObject;
}

/**
 * Create an instance of the filter for a particular service
 * within MaxScale.
 *
 * @param options	The options for this filter
 * @param params	The array of name/value pair parameters for the filter
 *
 * @return The instance data for this new instance
 */
static FILTER *createInstance(char **options, FILTER_PARAMETER **params) {
        ZENDESK_INSTANCE *my_instance;

        SERVICE *service;
        char *service_name, *service_param;
        int i, nservices = 0;

	my_instance = calloc(1, sizeof(ZENDESK_INSTANCE));
        my_instance->sessions = 0;

        my_instance->downstreams = calloc(1, sizeof(SERVICE *));

        my_instance->account_monitor = monitor_find("account monitor");

        if(my_instance->account_monitor == NULL) {
                skygw_log_write(LOGFILE_TRACE, "shardfilter: could not find account monitor", service_param);
        }

        for(i = 0; params[i]; i++) {
                if(strcasecmp(params[i]->name, "shard_format") == 0) {
                        my_instance->shard_format = strdup(params[i]->value);
                }

                if(strcasecmp(params[i]->name, "shard_services") == 0) {
                        service_param = strdup(params[i]->value);
                        skygw_log_write(LOGFILE_TRACE, "shardfilter: services %s", service_param);

                        while((service_name = strsep(&service_param, ",")) != NULL) {
                                service = service_find(service_name);
                                my_instance->downstreams = realloc(my_instance->downstreams, sizeof(SERVICE *) * (nservices + 2));
                                my_instance->downstreams[nservices++] = service;
                        }

                        free(service_param);
                }
        }

        if(my_instance->shard_format == NULL) {
                // default format of shard_%d
                my_instance->shard_format = calloc(9, sizeof(char));
                strcpy(my_instance->shard_format, "shard_%d");
        }

        my_instance->downstreams[nservices] = NULL;

	return (FILTER *) my_instance;
}

/**
 * Associate a new session with this instance of the filter.
 *
 * @param instance	The filter instance data
 * @param session	The session itself
 * @return Session specific data for this session
 */
static void *newSession(FILTER *instance, SESSION *session) {
        ZENDESK_SESSION *my_session = calloc(1, sizeof(ZENDESK_SESSION));
        my_session->rses = session;

        ZENDESK_INSTANCE *my_instance = (ZENDESK_INSTANCE *) instance;
        my_instance->sessions++;

	return my_session;
}

/**
 * Close a session with the filter, this is the mechanism
 * by which a filter may cleanup data structure etc.
 *
 * @param instance	The filter instance data
 * @param session	The session being closed
 */
static void closeSession(FILTER *instance, void *session) {
}

/**
 * Free the memory associated with this filter session.
 *
 * @param instance	The filter instance data
 * @param session	The session being closed
 */
static void freeSession(FILTER *instance, void *session) {
        free(session);
}

/**
 * Set the downstream component for this filter.
 *
 * @param instance	The filter instance data
 * @param session	The session being closed
 * @param downstream	The downstream filter or router
 */
static void setDownstream(FILTER *instance, void *session, DOWNSTREAM *downstream) {
        // set a default downstream to auth against
        ZENDESK_SESSION *my_session = (ZENDESK_SESSION *) session;
	my_session->shard_server = *downstream;
}

/**
 * The routeQuery entry point. This is passed the query buffer
 * to which the filter should be applied. Once applied the
 * query should normally be passed to the downstream component
 * (filter or router) in the filter chain.
 *
 * @param instance	The filter instance data
 * @param session	The filter session
 * @param queue		The query data
 */
static int routeQuery(FILTER *instance, void *session, GWBUF *queue) {
        ZENDESK_INSTANCE *zd_instance = (ZENDESK_INSTANCE *)instance;
        ZENDESK_SESSION *my_session = (ZENDESK_SESSION *) session;

        spinlock_init(&my_session->lock);
        spinlock_acquire(&my_session->lock);

        uint8_t *bufdata = GWBUF_DATA(queue);

        if(MYSQL_GET_COMMAND(bufdata) == MYSQL_COM_INIT_DB) {
                unsigned int qlen = MYSQL_GET_PACKET_LEN(bufdata);

                if(qlen > 8 && qlen < MYSQL_DATABASE_MAXLEN + 1) {
                        int account_id = shardfilter_find_account((char *) bufdata + 5, qlen);

                        if(account_id > 0) {
                                int shard_id = shardfilter_find_shard(zd_instance, account_id);

                                if(shard_id <= 0) {
                                        char errmsg[2048];
                                        snprintf(errmsg, 2048, "Could not find shard for account %d", account_id);
                                        GWBUF *err = modutil_create_mysql_err_msg(1, 0, 1046, "3D000", errmsg);
                                        return my_session->rses->client->func.write(my_session->rses->client, err);
                                }

                                char shard_database_id[MYSQL_DATABASE_MAXLEN + 1];
                                snprintf(shard_database_id, MYSQL_DATABASE_MAXLEN, zd_instance->shard_format, shard_id);

                                // find downstream
                                skygw_log_write(LOGFILE_TRACE, "shardfilter: finding %s", shard_database_id);
                                SERVICE *service = shardfilter_service_for_shard(zd_instance, shard_database_id);

                                if(service == NULL) {
                                        char errmsg[2048];
                                        snprintf((char *) &errmsg, 2048, "Could not find shard %d for account %d", shard_id, account_id);
                                        GWBUF *err = modutil_create_mysql_err_msg(1, 0, 1046, "3D000", errmsg);
                                        return my_session->rses->client->func.write(my_session->rses->client, err);
                                }

                                ROUTER_OBJECT *router = service->router;

                                if(my_session->shard_server.session) {
                                        // router->freeSession(my_session->shard_server.instance, my_session->shard_server.session);
                                }


                                SESSION *session = session_alloc(service, my_session->rses->client);

                                my_session->shard_server.instance = (void *) service->router_instance;
                                my_session->shard_server.session = session->router_session;
                                my_session->shard_server.routeQuery = (void *) router->routeQuery;
                                my_session->shard_id = shard_id;

                                // XXX: modutil_replace_SQL checks explicitly for COM_QUERY
                                // but just generically replaces the GWBUF data
                                bufdata[4] = MYSQL_COM_QUERY;

                                queue = modutil_replace_SQL(queue, shard_database_id);
                                queue = gwbuf_make_contiguous(queue);

                                ((uint8_t *) queue->start)[4] = MYSQL_COM_INIT_DB;
                        }
                }
        }


        spinlock_release(&my_session->lock);

        return my_session->shard_server.routeQuery(my_session->shard_server.instance, my_session->shard_server.session, queue);
}



/**
 * Diagnostics routine
 *
 * If fsession is NULL then print diagnostics on the filter
 * instance as a whole, otherwise print diagnostics for the
 * particular session.
 *
 * @param	instance	The filter instance
 * @param	fsession	Filter session, may be NULL
 * @param	dcb		The DCB for diagnostic output
 */
static void diagnostic(FILTER *instance, void *session, DCB *dcb) {
        ZENDESK_INSTANCE *my_instance = (ZENDESK_INSTANCE *) instance;
        ZENDESK_SESSION *my_session = (ZENDESK_SESSION *) session;

        dcb_printf(dcb, "using shard_format: %s", my_instance->shard_format);

        SERVICE *downstream;
        int i = 0;

        while((downstream = my_instance->downstreams[i++]) != NULL) {
                dcb_printf(dcb, "serving shard %s", downstream->name);
        }

        if(my_session != NULL) {
                dcb_printf(dcb, "current session is connected to %s", my_session->shard_id);
        }
}

SERVICE *shardfilter_service_for_shard(ZENDESK_INSTANCE *instance, char *name) {
        SERVICE *downstream;
        int i = 0;

        while((downstream = instance->downstreams[i++])) {
                if(strcasecmp(downstream->name, name) == 0) {
                        skygw_log_write(LOGFILE_TRACE, "shardfilter: found %s", name);
                        return downstream;
                }
        }

        return NULL;
}

int shardfilter_find_account(char *bufdata, int qlen) {
        int account_id = 0;
        char database_name[qlen];
        strncpy(database_name, bufdata, qlen - 1);
        database_name[qlen - 1] = 0;

        if(strncmp("account_", database_name, 8) == 0) {
                account_id = strtol(database_name + 8, NULL, 0);
        }

        return account_id;
}

int shardfilter_find_shard(ZENDESK_INSTANCE *instance, int account_id) {
        MYSQL_MONITOR *handle = (MYSQL_MONITOR *) instance->account_monitor->handle;

        if(handle == NULL || handle->accounts == NULL)
                return 0;

        int i = 0, *account;

        int *shard_id = (int *) hashtable_fetch(handle->accounts, &account_id);

        if(shard_id == NULL) {
                skygw_log_write(LOGFILE_TRACE, "shardfilter: could not find shard id for account_id %d", account_id);

                return 0;
        } else {
                skygw_log_write(LOGFILE_TRACE, "shardfilter: found shard_id %d for account_id %d", *shard_id, account_id);
                return *shard_id;
        }
}
