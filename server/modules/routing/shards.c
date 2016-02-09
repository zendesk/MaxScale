#include "account_mon.h"
#include "router.h"
#include "modinfo.h"
#include "modutil.h"
#include "monitor.h"
#include "mysql_client_server_protocol.h"
#include "query_classifier.h"

#include "readwritesplit.h"

#include <stdio.h>

static char *version_str = "V1.0.0";

MODULE_INFO info =
{
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
static int getCapabilities(ROUTER *, void *);
static void handleError(ROUTER *, void *, GWBUF *, DCB *, error_action_t, bool *);

static ROUTER_OBJECT MyObject =
{
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

typedef struct
{
    char *shard_format;

    SERVICE **downstreams;

    MONITOR *account_monitor;
} SHARD_ROUTER;

typedef struct
{
    SESSION *session;
    SERVICE *service;
    void *router_session;
    ROUTER *router_instance;
} SHARD_DOWNSTREAM;

typedef struct
{
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
static int shards_dcb_write(DCB *, GWBUF *);
static GWBUF *shards_replace_db_name(GWBUF *, char *);
static uintptr_t shards_handle_change_db(SHARD_ROUTER *, SHARD_SESSION *, GWBUF **);

#define SHARDS_SEND_ERROR(format, ...) \
    gwbuf_free(queue); \
char errmsg[2048]; \
snprintf(errmsg, 2048, format, ##__VA_ARGS__); \
return shards_send_error(shard_session, errmsg);

char *version()
{
    return version_str;
}

void ModuleInit()
{
}

ROUTER_OBJECT *GetModuleObject()
{
    return &MyObject;
}

static ROUTER *createInstance(SERVICE *service, char **options)
{
    // shard_format = options[0]
    // account_monitor = options[1]
    // unsharded service = options[2]
    // shards = options[3..]
    int i = 0;
    while (options[i] != NULL) { i++; }

    if (i < 2)
    {
        MXS_ERROR("shards: not enough router_options. expected shard_format,account_monitor,unsharded service,*shards");
        return NULL;
    }

    SHARD_ROUTER *router = calloc(1, sizeof(SHARD_ROUTER));

    if (router == NULL)
    {
        return NULL;
    }

    router->shard_format = strdup(options[0]);
    router->account_monitor = monitor_find(options[1]);

    if (router->account_monitor == NULL)
    {
        MXS_ERROR( "shards: could not find account monitor '%s'", options[1]);
    }

    char *shard;
    SERVICE *shard_service;

    i = 0;

    while ((shard = options[i + 2]) != NULL)
    {
        MXS_DEBUG("shards: services %s", shard);

        shard_service = service_find(shard);

        if (shard_service == NULL)
        {
            MXS_ERROR("shards: could not find service '%s'", shard);
        }
        else
        {
            void *new_downstreams = realloc(router->downstreams, sizeof(SERVICE *) * (i + 2));

            if (new_downstreams == NULL)
            {
                MXS_ERROR("shards: error allocating downstreams");

                free(router->downstreams);
                free(router);

                return NULL;
            }
            else
            {
                router->downstreams = new_downstreams;
            }

            router->downstreams[i] = shard_service;
        }

        i++;
    }

    router->downstreams[i] = NULL;

    return (ROUTER *) router;
}

static void *newSession(ROUTER *instance, SESSION *session)
{
    SHARD_ROUTER *router = (SHARD_ROUTER *) instance;

    DCB *original_dcb = session->client;

    MYSQL_session *mysql_session = original_dcb->data;

    uintptr_t init_shard_id = 0;
    SERVICE *init_downstream = router->downstreams[0];

    if (mysql_session->db != NULL && strlen(mysql_session->db) > 0)
    {
        uintptr_t account_id = 0;

        if ((account_id = shards_find_account(mysql_session->db, strlen(mysql_session->db) + 1)) > 0)
        {
            uintptr_t shard_id = shards_find_shard(router, account_id);

            if (shard_id > 0)
            {
                char shard_database_id[MYSQL_DATABASE_MAXLEN];
                snprintf(shard_database_id, MYSQL_DATABASE_MAXLEN, router->shard_format, shard_id);

                MXS_DEBUG("shards: finding %s", shard_database_id);
                SERVICE *service = shards_service_for_shard(router, shard_database_id);

                if (service != NULL)
                {
                    strcpy(mysql_session->db, shard_database_id);
                    init_downstream = service;
                    init_shard_id = shard_id;
                }
            }
        }
        else if (strncmp(mysql_session->db, "account", 7) == 0)
        {
            strcpy(mysql_session->db, init_downstream->name);
        }
    }

    DCB *cloned_dcb = dcb_clone(session->client);

    if (cloned_dcb == NULL)
    {
        MXS_ERROR("shards: error allocating new DCB for downstream session");
        return NULL;
    }

    spinlock_acquire(&cloned_dcb->writeqlock);

    // Set a default downstream
    SESSION *downstream = session_alloc(init_downstream, cloned_dcb);

    if (downstream == NULL)
    {
        // XXX DCB is added to the zombie queue and freed from there?
        MXS_ERROR("shards: error allocating default downstream session");
        return NULL;
    }

    SHARD_SESSION *shard_session = calloc(1, sizeof(SHARD_SESSION));
    shard_session->client_session = session;
    shard_session->shard_id = init_shard_id;

    shards_set_downstream(shard_session, downstream);

    cloned_dcb->func.write = shards_dcb_write;

    spinlock_release(&cloned_dcb->writeqlock);

    return (SESSION *) shard_session;
}

static void closeSession(ROUTER *instance, void *session)
{
    SHARD_SESSION *shard_session = (SHARD_SESSION *) session;
    shards_close_downstream_session(shard_session->downstream);
}

static void freeSession(ROUTER *instance, void *session)
{
    SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

    shards_free_downstream(shard_session->downstream);
    free(shard_session);
}

static int routeQuery(ROUTER *instance, void *session, GWBUF *queue)
{
    SHARD_ROUTER *shard_router = (SHARD_ROUTER *) instance;
    SHARD_SESSION *shard_session = (SHARD_SESSION *) session;

    // This is stolen from readwritesplit.c
    // Packets can come in incomplete, so we need to try
    // and reconstruct them before processing or pushing off until
    // more data comes in
    //
    // TODO: can this be solved by switching to statement based processing?
    // But will that cause problems with downstream packet based processing?
    if (GWBUF_IS_TYPE_UNDEFINED(queue))
    {
        GWBUF *tmpqueue = queue;

        do
        {
            if ((queue = modutil_get_next_MySQL_packet(&tmpqueue)) == NULL)
            {
                if (GWBUF_LENGTH(tmpqueue) > 0)
                {
                    DCB *dcb = shard_session->client_session->client;
                    dcb->dcb_readqueue = gwbuf_append(dcb->dcb_readqueue, tmpqueue);
                }

                // We haven't read the complete packet, re-append to the readqueue and return
                return 1;
            }

            gwbuf_set_type(queue, GWBUF_TYPE_MYSQL);
            gwbuf_set_type(queue, GWBUF_TYPE_SINGLE_STMT);
        } while (tmpqueue != NULL);			
    }

    uint8_t *bufdata = GWBUF_DATA(queue);
    uintptr_t account_id = 0;

    if (modutil_is_SQL(queue))
    {
        if (qc_get_operation(queue) == QUERY_OP_CHANGE_DB)
        {
            account_id = shards_handle_change_db(shard_router, shard_session, &queue);
        }
    }
    else if (MYSQL_IS_COM_INIT_DB(bufdata))
    {
        unsigned int qlen = MYSQL_GET_PACKET_LEN(bufdata);

        if (qlen > 8 && qlen < MYSQL_DATABASE_MAXLEN + 1)
        {
            account_id = shards_find_account((char *) bufdata + 5, qlen);
        }
        else if (qlen >= 7 && strncmp((char *) bufdata + 5, "account", 7) == 0)
        {
            queue = shards_replace_db_name(queue, shard_router->downstreams[0]->name);
            shard_session->shard_id = 0;
        }
    }

    if (account_id > 0)
    {
        uintptr_t shard_id = shards_find_shard(shard_router, account_id);

        if (shard_id < 1)
        {
            SHARDS_SEND_ERROR("Could not find shard for account %" PRIuPTR, account_id);
        }

        char shard_database_id[MYSQL_DATABASE_MAXLEN];
        snprintf(shard_database_id, MYSQL_DATABASE_MAXLEN, shard_router->shard_format, shard_id);

        // find downstream
        MXS_DEBUG("shards: finding %s", shard_database_id);
        SERVICE *service = shards_service_for_shard(shard_router, shard_database_id);

        if (service == NULL)
        {
            SHARDS_SEND_ERROR("Could not find shard %" PRIuPTR " for account %" PRIuPTR, shard_id, account_id);
        }

        if (service != shard_session->downstream.service && !shards_switch_session(shard_session, service))
        {
            SHARDS_SEND_ERROR("Error allocating new session for shard %" PRIuPTR, shard_id);
        }

        shard_session->shard_id = shard_id;
        queue = shards_replace_db_name(queue, shard_database_id);
    }
    else if (shard_session->shard_id == 0)
    {
        if (shard_session->downstream.service != shard_router->downstreams[0] && !shards_switch_session(shard_session, shard_router->downstreams[0]))
        {
            // TODO close session?
            SHARDS_SEND_ERROR("Error switching to default shard");
        }
    }


    SHARD_DOWNSTREAM downstream = shard_session->downstream;
    DOWNSTREAM head = downstream.session->head;

    return head.routeQuery(head.instance, head.session, queue);
}

static void clientReply(ROUTER *instance, void *session, GWBUF *queue, DCB *backend_dcb)
{
}

static void diagnostic(ROUTER *instance, DCB *dcb)
{
}

static int getCapabilities(ROUTER *inst, void *router_session)
{
    return 0;
}

static void handleError(ROUTER *instance, void *router_session, GWBUF *errbuf, DCB *backend_dcb, error_action_t action, bool *succp)
{
}

static SERVICE *shards_service_for_shard(SHARD_ROUTER *instance, char *name)
{
    SERVICE *downstream;
    int i = 0;

    while ((downstream = instance->downstreams[i++]) != NULL)
    {
        if (strcasecmp(downstream->name, name) == 0)
        {
            MXS_DEBUG("shards: found %s", name);
            return downstream;
        }
    }

    return NULL;
}

static uintptr_t shards_find_account(char *bufdata, int qlen)
{
    uintptr_t account_id = 0;
    char database_name[qlen];
    strncpy(database_name, bufdata, qlen - 1);
    database_name[qlen - 1] = '\0';

    if (strncmp("account_", database_name, 8) == 0)
    {
        account_id = strtol(database_name + 8, NULL, 0);
    }

    return account_id;
}

static uintptr_t shards_find_shard(SHARD_ROUTER *instance, uintptr_t account_id)
{
    if (instance->account_monitor == NULL)
        return 0;

    ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) instance->account_monitor->handle;

    if (handle == NULL)
        return 0;

    return account_monitor_find_shard(handle, account_id);
}

static void shards_free_downstream(SHARD_DOWNSTREAM downstream)
{
    // Backend DCBs need to be closed and will free the session
}

static void shards_close_downstream_session(SHARD_DOWNSTREAM downstream)
{
    SESSION *session = downstream.session;

    spinlock_acquire(&session->ses_lock);

    if (session->state == SESSION_STATE_STOPPING || session->state == SESSION_STATE_TO_BE_FREED || session->state == SESSION_STATE_FREE)
    {
        spinlock_release(&session->ses_lock);
        return;
    }

    // Make sure the downstream is "STOPPING"
    session->state = SESSION_STATE_STOPPING;
    // Detach this session, session_free will actually free the session
    session->ses_is_child = false;
    // We continue to pass this client around
    session->client = NULL;
    // We don't want session_free removing our MYSQL_session
    MYSQL_session *new_session = calloc(1, sizeof(MYSQL_session));
    memcpy(new_session, session->data, sizeof(MYSQL_session));
    session->data = new_session;

    spinlock_release(&session->ses_lock);

    // And we don't want the router session linking to it
    session_unlink_dcb(session, NULL);

    downstream.service->router->closeSession(downstream.router_instance, downstream.router_session);
}

static void shards_set_downstream(SHARD_SESSION *shard_session, SESSION *session)
{
    session->parent = shard_session->client_session;

    shard_session->downstream.session = session;
    shard_session->downstream.service = session->service;
    shard_session->downstream.router_session = session->router_session;

    ROUTER_CLIENT_SES *router_session = (ROUTER_CLIENT_SES *) session->router_session;
    shard_session->downstream.router_instance = (ROUTER *) router_session->router;
}

static int shards_send_error(SHARD_SESSION *shard_session, char *errmsg)
{
    GWBUF *err = modutil_create_mysql_err_msg(1, 0, 1046, "3D000", errmsg);

    DCB *client = shard_session->client_session->client;
    return client->func.write(client, err);
}

static bool shards_switch_session(SHARD_SESSION *shard_session, SERVICE *service)
{
    SHARD_DOWNSTREAM downstream = shard_session->downstream;
    DCB *cloned_dcb = downstream.session->client;

    shards_close_downstream_session(downstream);
    shards_free_downstream(downstream);

    SESSION *new_session = session_alloc(service, cloned_dcb);

    if (new_session == NULL)
    {
        return false;
    }

    CHK_SESSION(new_session);

    shards_set_downstream(shard_session, new_session);

    return true;
}

static int shards_dcb_write(DCB *dcb, GWBUF *queue)
{
    SESSION *session = dcb->session;
    SESSION *parent = session->parent;
    DCB *actual_dcb = parent->client;

    return actual_dcb->func.write(actual_dcb, queue);
}

static GWBUF *shards_replace_db_name(GWBUF *queue, char *database_name)
{
    char *query = database_name;
    bool is_query = modutil_is_SQL(queue);

    if (is_query)
    {
        query = calloc(strlen(database_name) + 5, sizeof(char));
        // TODO: in this case should we do anything for the error handling?
        // the replace will fail, but so will the database selection
        if (query == NULL)
        {
            return queue;
        }
        memcpy(query, "USE ", sizeof(char) * 4);
        memcpy(query + 4, database_name, sizeof(char) * strlen(database_name));
    }
    else
    {
        ((uint8_t *) queue->start)[4] = MYSQL_COM_QUERY;
    }

    queue = modutil_replace_SQL(queue, query);
    queue = gwbuf_make_contiguous(queue);

    if (!is_query)
    {
        ((uint8_t *) queue->start)[4] = MYSQL_COM_INIT_DB;
    }

    return queue;
}

static uintptr_t shards_handle_change_db(SHARD_ROUTER *shard_router, SHARD_SESSION *shard_session, GWBUF **queue)
{
    // Based on sharding_common.c
    char *query = modutil_get_SQL(*queue);

    if (query == NULL)
    {
        return 0;
    }

    char *saved = NULL;
    char *token = strtok_r(query, " ;", &saved);

    if (strcasecmp(token, "use") != 0)
    {
        return 0;
    }

    token = strtok_r(NULL, " ;", &saved);

    // account is the shortest match at 7 chars
    if (token == NULL || strlen(token) < 8)
    {
        return 0;
    }

    if (strncmp(token, "`account`", 9) == 0 || strncmp(token, "account", 7) == 0)
    {
        *queue = shards_replace_db_name(*queue, shard_router->downstreams[0]->name);
        shard_session->shard_id = 0;

        return 0;
    }

    int len = 0;
    // Quoted table name: USE `account_1`
    if (token[0] == '`')
    {
        // walk until we get another ` or \0
        int i = 1;
        while (token[i] != '`' && token[i] != '\0') { i++; }
        return shards_find_account(&token[1], i);
    }
    else
    {
        return shards_find_account(token, strlen(token) + 1);
    }
}
