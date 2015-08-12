#include "router.h"
#include "modinfo.h"
#include "monitor.h"

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
        SERVICE *default_downstream;

        MONITOR *account_monitor;
} SHARD_ROUTER;

typedef struct {
        SESSION *client_session;

        SESSION *downstream;

        int shard_id;
} SHARD_SESSION;

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

        for(i = 2; (shard = options[i]) != NULL; i++) {
                skygw_log_write(LOGFILE_DEBUG, "shardfilter: services %s", shard);

                shard_service = service_find(shard);

                // TODO
                if(shard_service == NULL) {
                } else {
                        router->downstreams = realloc(router->downstreams, sizeof(SERVICE *) * (i - 1));
                        // TODO null
                        router->downstreams[i - 2] = shard_service;
                }
        }

        router->downstreams[i - 1] = NULL;
        router->default_downstream = router->downstreams[0];

        return (ROUTER *) router;
}

static void *newSession(ROUTER *instance, SESSION *session) {
        SHARD_ROUTER *router = (SHARD_ROUTER *) instance;
        SHARD_SESSION *shard_session = malloc(sizeof(SHARD_SESSION));

        // Set a default downstream
        DCB *original_dcb = session->client;
        DCB *cloned_dcb = dcb_clone(session->client);
        // TODO null

        session->client = cloned_dcb;
        SESSION *downstream = session_alloc(router->default_downstream, original_dcb);
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

        downstream_service->router->closeSession(router_instance, (void *) router_session);;
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
        SHARD_ROUTER *router = (SHARD_ROUTER *) instance;
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        /*
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
                                        spinlock_release(&my_session->lock);
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
                                        spinlock_release(&my_session->lock);
                                        return my_session->rses->client->func.write(my_session->rses->client, err);
                                }

                                ROUTER_OBJECT *router = service->router;
                                DCB *dcb = my_session->rses->client;
                                SESSION *new_session = session_alloc(service, dcb);

                                if(new_session == NULL) {
                                        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, "shardfilter: error allocating session, terminating")));

                                        freeSession(instance, session);
                                        shardfilter_free_client_session(my_session->rses);

                                        my_session = NULL;

                                        spinlock_release(&my_session->lock);

                                        skygw_log_write(LOGFILE_TRACE, "shardfilter: session does not exist -- returning");
                                        gwbuf_free(queue);
                                        return 0;
                                }

                                CHK_SESSION(new_session);

                                spinlock_acquire(&my_session->rses->ses_lock);

                                // done by session_unlink_dcb
                                atomic_add(&my_session->rses->refcount, -1);
                                my_session->rses->client = NULL;
                                // this is a reference to the dcb data

                                void *data = malloc(sizeof(MYSQL_session));

                                if(data == NULL) {
                                        // well, shit
                                }

                                memcpy(data, my_session->rses->data, sizeof(MYSQL_session));
                                my_session->rses->data = data;

                                spinlock_release(&my_session->rses->ses_lock);

                                DOWNSTREAM shard;
                                shard.instance = (void *) service->router_instance;
                                shard.session = new_session->router_session;
                                shard.routeQuery = (void *) router->routeQuery;

                                // XXX: modutil_replace_SQL checks explicitly for COM_QUERY
                                // but just generically replaces the GWBUF data
                                bufdata[4] = MYSQL_COM_QUERY;

                                queue = modutil_replace_SQL(queue, shard_database_id);
                                queue = gwbuf_make_contiguous(queue);

                                ((uint8_t *) queue->start)[4] = MYSQL_COM_INIT_DB;

                                int retval = shard.routeQuery(shard.instance, shard.session, queue);

                                spinlock_release(&my_session->lock);

                                // clean up the current session+filter chain
                                // since we've alloc-ed a new session+filter chain
                                shardfilter_close_client_session(my_session->rses);
                                shardfilter_free_client_session(my_session->rses);

                                return retval;
                        }
                }
        } */

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
