#include "account_mon.h"
#include "router.h"
#include "modinfo.h"
#include "modutil.h"
#include "monitor.h"
#include "mysql_client_server_protocol.h"

#include "readwritesplit.h"

#include <stdio.h>

static char *version_str = "V1.0.0";

MODULE_INFO info = {
        MODULE_API_ROUTER,
        MODULE_IN_DEVELOPMENT,
        ROUTER_VERSION,
        "Zendesk's custom shard router"
};

static ROUTER *createInstance(SERVICE *, char **);
static void *newSession(ROUTER *, SESSION *);
static void closeSession(ROUTER *, void *);
static void freeSession(ROUTER *, void *);
static int routeQuery(ROUTER *, void *, GWBUF *);
static void clientReply(ROUTER *, void *, GWBUF *, DCB *);
static void diagnostic(ROUTER *, DCB *);
static uint8_t getCapabilities(ROUTER *, void *);
static void handleError(ROUTER *, void *, GWBUF *, DCB *, error_action_t, bool *);

static ROUTER_OBJECT MyObject = {
        createInstance,
        newSession,
        closeSession,
        freeSession,
        routeQuery,
        diagnostic,
        clientReply,
        handleError,
        getCapabilities
};

typedef struct {
        char *shard_format;

        SERVICE **downstreams;

        MONITOR *account_monitor;
} SHARD_ROUTER;

typedef struct {
        SESSION *client_session;

        SESSION *downstream;

        int shard_id;
} SHARD_SESSION;

static SERVICE *shards_service_for_shard(SHARD_ROUTER *, char *);
static int shards_find_shard(SHARD_ROUTER *, long long int);
static int shards_find_account(char *, int);

char *version() {
        return version_str;
}

void ModuleInit() {
}

ROUTER_OBJECT *GetModuleObject() {
        return &MyObject;
}

static ROUTER *createInstance(SERVICE *service, char **options) {
        SHARD_ROUTER *router = malloc(sizeof(SHARD_ROUTER));

        // shard_format = options[0]
        // account_monitor = options[1]
        // unsharded service = options[2]
        // shards = options[3..]
        // TODO check length

        router->shard_format = strdup(options[0]);
        router->account_monitor = monitor_find(options[1]);

        if(router->account_monitor == NULL) {
                skygw_log_write(LOGFILE_ERROR, "shards: could not find account monitor '%s'", options[1]);
        }

        int i;
        char *shard;
        SERVICE *shard_service;

        router->downstreams = calloc(1, sizeof(SERVICE *));

        for(i = 2; (shard = options[i]) != NULL; i++) {
                skygw_log_write(LOGFILE_DEBUG, "shardfilter: services %s", shard);

                shard_service = service_find(shard);

                // TODO
                if(shard_service == NULL) {
                } else {
                        if(i > 2) {
                                router->downstreams = realloc(router->downstreams, sizeof(SERVICE *) * (i - 1));
                        }

                        // TODO null
                        router->downstreams[i - 2] = shard_service;
                }
        }

        router->downstreams[i - 2] = NULL;

        return (ROUTER *) router;
}

static void *newSession(ROUTER *instance, SESSION *session) {
        SHARD_ROUTER *router = (SHARD_ROUTER *) instance;
        SHARD_SESSION *shard_session = malloc(sizeof(SHARD_SESSION));

        // Set a default downstream
        DCB *original_dcb = session->client;
        DCB *cloned_dcb = dcb_clone(session->client);
        cloned_dcb->func.write = original_dcb->func.write;
        cloned_dcb->fd = original_dcb->fd;
        // TODO null

        SESSION *downstream = session_alloc(router->downstreams[0], cloned_dcb);
        // TODO null

        shard_session->downstream = downstream;
        shard_session->shard_id = 0;
        shard_session->client_session = session;

        return (SESSION *) shard_session;
}

static void closeSession(ROUTER *instance, void *session) {
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        SESSION *downstream = shard_session->downstream;
        SERVICE *downstream_service = downstream->service;
        ROUTER_CLIENT_SES *router_session = (ROUTER_CLIENT_SES *) downstream->router_session;
        ROUTER *router_instance = (ROUTER *) router_session->router;

        // Make sure the downstream is "STOPPING"
        downstream->state = SESSION_STATE_STOPPING;

        downstream_service->router->closeSession(router_instance, (void *) router_session);
}

static void freeSession(ROUTER *instance, void *session) {
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        SESSION *downstream = shard_session->downstream;
        SERVICE *downstream_service = downstream->service;
        ROUTER_CLIENT_SES *router_session = (ROUTER_CLIENT_SES *) downstream->router_session;
        ROUTER *router_instance = (ROUTER *) router_session->router;

        downstream_service->router->freeSession(router_instance, (void *) router_session);;

        // ? free(downstream);
        free(shard_session);
}

static int routeQuery(ROUTER *instance, void *session, GWBUF *queue) {
        SHARD_ROUTER *shard_router = (SHARD_ROUTER *) instance;
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        uint8_t *bufdata = GWBUF_DATA(queue);

        if(MYSQL_GET_COMMAND(bufdata) == MYSQL_COM_INIT_DB) {
                unsigned int qlen = MYSQL_GET_PACKET_LEN(bufdata);

                if(qlen > 8 && qlen < MYSQL_DATABASE_MAXLEN + 1) {
                        int account_id = shards_find_account((char *) bufdata + 5, qlen);

                        if(account_id > 0) {
                                int shard_id = shards_find_shard(shard_router, account_id);

                                if(shard_id <= 0) {
                                        char errmsg[2048];
                                        snprintf(errmsg, 2048, "Could not find shard for account %d", account_id);
                                        GWBUF *err = modutil_create_mysql_err_msg(1, 0, 1046, "3D000", errmsg);
                                        // TODO
                                }

                                char shard_database_id[MYSQL_DATABASE_MAXLEN + 1];
                                snprintf(shard_database_id, MYSQL_DATABASE_MAXLEN, shard_router->shard_format, shard_id);

                                // find downstream
                                skygw_log_write(LOGFILE_TRACE, "shardfilter: finding %s", shard_database_id);
                                SERVICE *service = shards_service_for_shard(shard_router, shard_database_id);

                                if(service == NULL) {
                                        char errmsg[2048];
                                        snprintf((char *) &errmsg, 2048, "Could not find shard %d for account %d", shard_id, account_id);
                                        GWBUF *err = modutil_create_mysql_err_msg(1, 0, 1046, "3D000", errmsg);
                                        // TODO
                                }

                                SESSION *current_downstream = shard_session->downstream;
                                SERVICE *current_downstream_service = current_downstream->service;
                                ROUTER_CLIENT_SES *current_router_session = (ROUTER_CLIENT_SES *) current_downstream->router_session;
                                ROUTER *current_router_instance = (ROUTER *) current_router_session->router;
                                SESSION *new_session = session_alloc(service, current_downstream->client);

                                if(new_session == NULL) {
                                        // TODO
                                }

                                CHK_SESSION(new_session);

                                current_downstream->client = NULL;
                                current_downstream->state = SESSION_STATE_STOPPING;
                                current_downstream_service->router->closeSession(current_router_instance, (void *) current_router_session);

                                // TODO memory leak!?
                                // readwritesplit closes its underlying backend DCBs, but they're still included in the refcount
                                // session_unlink_dcb is never called, so session_free's refcount check fails
                                // session_free(current_downstream);

                                shard_session->downstream = new_session;
                                shard_session->shard_id = shard_id;

                                // XXX: modutil_replace_SQL checks explicitly for COM_QUERY
                                // but just generically replaces the GWBUF data
                                bufdata[4] = MYSQL_COM_QUERY;

                                queue = modutil_replace_SQL(queue, shard_database_id);
                                queue = gwbuf_make_contiguous(queue);

                                ((uint8_t *) queue->start)[4] = MYSQL_COM_INIT_DB;
                        }
                }
        }

        SESSION *downstream = shard_session->downstream;
        SERVICE *downstream_service = downstream->service;
        ROUTER_CLIENT_SES *router_session = (ROUTER_CLIENT_SES *) downstream->router_session;
        ROUTER *router_instance = (ROUTER *) router_session->router;

        return downstream_service->router->routeQuery(router_instance, (void *) router_session, queue);
}

void clientReply(ROUTER *instance, void *session, GWBUF *queue, DCB *backend_dcb) {
}

static void diagnostic(ROUTER *instance, DCB *dcb) {
}

static uint8_t getCapabilities(ROUTER *inst, void *router_session) {
        return 0;
}

static void handleError(ROUTER *instance, void *router_session, GWBUF *errbuf, DCB *backend_dcb, error_action_t action, bool *succp) {
}

static SERVICE *shards_service_for_shard(SHARD_ROUTER *instance, char *name) {
        SERVICE *downstream;
        int i = 0;

        while((downstream = instance->downstreams[i++]) != NULL) {
                if(strcasecmp(downstream->name, name) == 0) {
                        skygw_log_write(LOGFILE_TRACE, "shardfilter: found %s", name);
                        return downstream;
                }
        }

        return NULL;
}

static int shards_find_account(char *bufdata, int qlen) {
        int account_id = 0;
        char database_name[qlen];
        strncpy(database_name, bufdata, qlen - 1);
        database_name[qlen - 1] = 0;

        if(strncmp("account_", database_name, 8) == 0) {
                account_id = strtol(database_name + 8, NULL, 0);
        }

        return account_id;
}

static int shards_find_shard(SHARD_ROUTER *instance, long long int account_id) {
        if(account_id == 1) {
                return 2;
        } else {
                return 1;
        }

        if(instance->account_monitor == NULL)
                return 0;

        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) instance->account_monitor->handle;

        if(handle == NULL || handle->accounts == NULL)
                return 0;

        int i = 0, *account;

        long long int shard_id = (long long int) hashtable_fetch(handle->accounts, (void *) account_id);

        if(shard_id == 0) {
                skygw_log_write(LOGFILE_TRACE, "shardfilter: could not find shard id for account %d", account_id);
                return 0;
        } else {
                skygw_log_write(LOGFILE_TRACE, "shardfilter: found shard_id %d for account %d", shard_id, account_id);
                return shard_id;
        }
}
