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
        SESSION *session;
        SERVICE *service;
        void *router_session;
        ROUTER *router_instance;
} SHARD_DOWNSTREAM;

typedef struct {
        SESSION *client_session;

        SHARD_DOWNSTREAM downstream;

        uintptr_t shard_id;
} SHARD_SESSION;

static SERVICE *shards_service_for_shard(SHARD_ROUTER *, char *);
static uintptr_t shards_find_shard(SHARD_ROUTER *, uintptr_t);
static uintptr_t shards_find_account(char *, int);
static void shards_free_downstream(SHARD_DOWNSTREAM);
static void shards_close_downstream_session(SHARD_DOWNSTREAM);
static void shards_set_downstream(SHARD_SESSION *, SESSION *);
static int shards_send_error(SHARD_SESSION *, char *);
static bool shards_switch_session(SHARD_SESSION *, SERVICE *);

char *version() {
        return version_str;
}

void ModuleInit() {
}

ROUTER_OBJECT *GetModuleObject() {
        return &MyObject;
}

static ROUTER *createInstance(SERVICE *service, char **options) {
        // shard_format = options[0]
        // account_monitor = options[1]
        // unsharded service = options[2]
        // shards = options[3..]
        int i = 0;
        while(options[i] != NULL) { i++; }

        if(i < 2) {
                skygw_log_write(LOGFILE_ERROR, "shards: not enough router_options. expected shard_format,account_monitor,unsharded service,*shards");
                return NULL;
        }

        SHARD_ROUTER *router = calloc(1, sizeof(SHARD_ROUTER));

        router->shard_format = strdup(options[0]);
        router->account_monitor = monitor_find(options[1]);

        if(router->account_monitor == NULL) {
                skygw_log_write(LOGFILE_ERROR, "shards: could not find account monitor '%s'", options[1]);
        }

        char *shard;
        SERVICE *shard_service;

        i = 0;

        while((shard = options[i + 2]) != NULL) {
                skygw_log_write(LOGFILE_DEBUG, "shards: services %s", shard);

                shard_service = service_find(shard);

                if(shard_service == NULL) {
                        skygw_log_write(LOGFILE_ERROR, "shards: could not find service '%s'", shard);
                } else {
                        void *new_downstreams = realloc(router->downstreams, sizeof(SERVICE *) * (i + 2));

                        if(new_downstreams == NULL) {
                                skygw_log_write(LOGFILE_ERROR, "shards: error allocating downstreams");

                                free(router->downstreams);
                                free(router);

                                return NULL;
                        } else {
                                router->downstreams = new_downstreams;
                        }

                        router->downstreams[i] = shard_service;
                }

                i++;
        }

        router->downstreams[i] = NULL;

        return (ROUTER *) router;
}

static void *newSession(ROUTER *instance, SESSION *session) {
        SHARD_ROUTER *router = (SHARD_ROUTER *) instance;

        DCB *original_dcb = session->client;
        DCB *cloned_dcb = dcb_clone(session->client);

        if(cloned_dcb == NULL) {
                skygw_log_write(LOGFILE_ERROR, "shards: error allocating new DCB for downstream session");
                return NULL;
        }

        cloned_dcb->func.write = original_dcb->func.write;
        cloned_dcb->fd = original_dcb->fd;

        // Set a default downstream
        SESSION *downstream = session_alloc(router->downstreams[0], cloned_dcb);

        if(downstream == NULL) {
                skygw_log_write(LOGFILE_ERROR, "shards: error allocating default downstream session");
                dcb_free(cloned_dcb);
                return NULL;
        }

        SHARD_SESSION *shard_session = malloc(sizeof(SHARD_SESSION));
        shards_set_downstream(shard_session, downstream);
        shard_session->shard_id = 0;
        shard_session->client_session = session;

        return (SESSION *) shard_session;
}

static void closeSession(ROUTER *instance, void *session) {
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        // The cloned DCB has the same FD so it will get closed twice
        // Still results in a "epoll_ctl could not remove, not found" error, since that
        // happens before we ever reach here, but this is deemed non-fatal inside poll_resolve_error
        shard_session->client_session->client->fd = DCBFD_CLOSED;

        shards_close_downstream_session(shard_session->downstream);
}

static void freeSession(ROUTER *instance, void *session) {
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        shards_free_downstream(shard_session->downstream);
        free(shard_session);
}

static int routeQuery(ROUTER *instance, void *session, GWBUF *queue) {
        SHARD_ROUTER *shard_router = (SHARD_ROUTER *) instance;
        SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

        uint8_t *bufdata = GWBUF_DATA(queue);

        if(MYSQL_GET_COMMAND(bufdata) == MYSQL_COM_INIT_DB) {
                unsigned int qlen = MYSQL_GET_PACKET_LEN(bufdata);
                uintptr_t account_id;

                if(qlen > 8 && qlen < MYSQL_DATABASE_MAXLEN + 1 && (account_id = shards_find_account((char *) bufdata + 5, qlen)) > 0) {
                        uintptr_t shard_id = shards_find_shard(shard_router, account_id);

                        if(shard_id < 1) {
                                gwbuf_free(queue);

                                char errmsg[2048];
                                snprintf((char *) &errmsg, 2048, "Could not find shard for account %" PRIuPTR, account_id);
                                return shards_send_error(shard_session, errmsg);
                        }

                        char *shard_database_id = alloca(sizeof(char) * (MYSQL_DATABASE_MAXLEN + 1));
                        snprintf(shard_database_id, MYSQL_DATABASE_MAXLEN, shard_router->shard_format, shard_id);

                        // find downstream
                        skygw_log_write(LOGFILE_TRACE, "shards: finding %s", shard_database_id);
                        SERVICE *service = shards_service_for_shard(shard_router, shard_database_id);

                        if(service == NULL) {
                                gwbuf_free(queue);

                                char errmsg[2048];
                                snprintf((char *) &errmsg, 2048, "Could not find shard %" PRIuPTR " for account %" PRIuPTR, shard_id, account_id);
                                return shards_send_error(shard_session, errmsg);
                        }

                        if(service != shard_session->downstream.service) {
                                if(!shards_switch_session(shard_session, service)) {
                                        gwbuf_free(queue);
                                        char errmsg[2048];
                                        snprintf((char *) &errmsg, 2048, "Error allocating new session for shard %" PRIuPTR, shard_id);
                                        return shards_send_error(shard_session, errmsg);
                                }

                                shard_session->shard_id = shard_id;
                        }

                        // XXX: modutil_replace_SQL checks explicitly for COM_QUERY
                        // but just generically replaces the GWBUF data
                        bufdata[4] = MYSQL_COM_QUERY;

                        queue = modutil_replace_SQL(queue, shard_database_id);
                        queue = gwbuf_make_contiguous(queue);

                        ((uint8_t *) queue->start)[4] = MYSQL_COM_INIT_DB;
                } else if(shard_session->downstream.service != shard_router->downstreams[0]) {
                        if(!shards_switch_session(shard_session, shard_router->downstreams[0])) {
                                gwbuf_free(queue);
                                char errmsg[2048];
                                snprintf((char *) &errmsg, 2048, "Error switching to default shard");
                                return shards_send_error(shard_session, errmsg);
                        }

                        shard_session->shard_id = 0;
                }
        }

        SHARD_DOWNSTREAM downstream = shard_session->downstream;
        return downstream.service->router->routeQuery(downstream.router_instance, downstream.router_session, queue);
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
                        skygw_log_write(LOGFILE_TRACE, "shards: found %s", name);
                        return downstream;
                }
        }

        return NULL;
}

static uintptr_t shards_find_account(char *bufdata, int qlen) {
        uintptr_t account_id = 0;
        char database_name[qlen];
        strncpy(database_name, bufdata, qlen - 1);
        database_name[qlen - 1] = 0;

        if(strncmp("account_", database_name, 8) == 0) {
                account_id = strtol(database_name + 8, NULL, 0);
        }

        return account_id;
}

static uintptr_t shards_find_shard(SHARD_ROUTER *instance, uintptr_t account_id) {
        if(instance->account_monitor == NULL)
                return 0;

        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) instance->account_monitor->handle;

        if(handle == NULL)
                return 0;

        return account_monitor_find_shard(handle, account_id);
}

static void shards_free_downstream(SHARD_DOWNSTREAM downstream) {
        downstream.service->router->freeSession(downstream.router_instance, downstream.router_session);

        // unlink DCB from session for final free
        if(downstream.session->client != NULL) {
                downstream.session->client->session = NULL;
        }

        session_free(downstream.session);
}

static void shards_close_downstream_session(SHARD_DOWNSTREAM downstream) {
        // Make sure the downstream is "STOPPING"
        downstream.session->state = SESSION_STATE_STOPPING;
        downstream.service->router->closeSession(downstream.router_instance, downstream.router_session);
}

static void shards_set_downstream(SHARD_SESSION *shard_session, SESSION *session) {
        shard_session->downstream.session = session;
        shard_session->downstream.service = session->service;
        shard_session->downstream.router_session = session->router_session;
        shard_session->downstream.router_instance = (ROUTER *) ((ROUTER_CLIENT_SES *) session->router_session)->router;
}

static int shards_send_error(SHARD_SESSION *shard_session, char *errmsg) {
        GWBUF *err = modutil_create_mysql_err_msg(1, 0, 1046, "3D000", errmsg);

        DCB *client = shard_session->client_session->client;
        return client->func.write(client, err);
}

static bool shards_switch_session(SHARD_SESSION *shard_session, SERVICE *service) {
        SHARD_DOWNSTREAM downstream = shard_session->downstream;
        SESSION *new_session = session_alloc(service, downstream.session->client);

        if(new_session == NULL) {
                return false;
        }

        CHK_SESSION(new_session);

        downstream.session->client = NULL;
        shards_close_downstream_session(downstream);
        shards_free_downstream(downstream);

        shards_set_downstream(shard_session, new_session);

        return true;
}
