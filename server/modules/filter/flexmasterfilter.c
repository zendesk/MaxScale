#include <stdio.h>
#include <filter.h>
#include <modinfo.h>
#include <modutil.h>
#include <atomic.h>

#include <service.h>
#include <readwritesplit.h>
#include <flexmaster.h>

MODULE_INFO info = {
    MODULE_API_FILTER,
    MODULE_BETA_RELEASE,
    FILTER_VERSION,
    "A filter that holds queries for Flexmaster"
};

static char *version_str = "V1.0.0";

static FILTER *createInstance(char **, FILTER_PARAMETER **);
static void *newSession(FILTER *, SESSION *);
static void closeSession(FILTER *, void *);
static void freeSession(FILTER *, void *);
static void setDownstream(FILTER *, void *, DOWNSTREAM *);
static int routeQuery(FILTER *, void *, GWBUF *);
static void diagnostic(FILTER *, void *, DCB *);
static void add_waiting_client(FLEXMASTER_FILTER_INSTANCE *, DCB *);

static FILTER_OBJECT MyObject = {
    createInstance,
    newSession,
    closeSession,
    freeSession,
    setDownstream,
    NULL, // No upstream requirement
    routeQuery,
    NULL,
    diagnostic,
};

typedef struct {
    DOWNSTREAM downstream;
    bool transaction_active;
    DCB *client;
} FLEXMASTER_FILTER_SESSION;

char *version() {
    return version_str;
}

void ModuleInit() {
}

FILTER_OBJECT *GetModuleObject() {
    return &MyObject;
}

static FILTER *createInstance(char **options, FILTER_PARAMETER **params) {
    FLEXMASTER_FILTER_INSTANCE *flex_instance;

    if((flex_instance = calloc(1, sizeof(FLEXMASTER_FILTER_INSTANCE))) != NULL) {
        spinlock_init(&flex_instance->transaction_lock);
        flex_instance->waiting_clients = calloc(1, sizeof(DCB *));
    }

    return (FILTER *) flex_instance;
}

static void *newSession(FILTER *instance, SESSION *session) {
    FLEXMASTER_FILTER_INSTANCE *flex_instance = (FLEXMASTER_FILTER_INSTANCE *) instance;
    FLEXMASTER_FILTER_SESSION *flex_session;

    if((flex_session = calloc(1, sizeof(FLEXMASTER_FILTER_SESSION))) != NULL) {
        flex_session->transaction_active = false;
        flex_session->client = session->client;
    }

    return flex_session;
}

static void closeSession(FILTER *instance, void *session) {
}

static void freeSession(FILTER *instance, void *session) {
    free(session);
}

static void setDownstream(FILTER *instance, void *session, DOWNSTREAM *downstream) {
    FLEXMASTER_FILTER_SESSION *flex_session = (FLEXMASTER_FILTER_SESSION *) session;
    flex_session->downstream = *downstream;
}

static int routeQuery(FILTER *instance, void *session, GWBUF *queue) {
    FLEXMASTER_FILTER_INSTANCE *flex_instance = (FLEXMASTER_FILTER_INSTANCE *) instance;
    FLEXMASTER_FILTER_SESSION *flex_session = (FLEXMASTER_FILTER_SESSION *) session;
    ROUTER_CLIENT_SES *router_session = (ROUTER_CLIENT_SES *) flex_session->downstream.session;

    // We want to hold this query, since a flexmaster swap is in progress
    if(!flex_session->transaction_active && SPINLOCK_IS_LOCKED(&flex_instance->transaction_lock)) {
        flex_session->client->dcb_readqueue = gwbuf_append(queue, flex_session->client->dcb_readqueue);
        add_waiting_client(flex_instance, flex_session->client);

        return 1;
    }

    int retval = flex_session->downstream.routeQuery(flex_session->downstream.instance, router_session, queue);

    if(router_session->rses_transaction_active != flex_session->transaction_active) {
        if(router_session->rses_transaction_active) {
            atomic_add(&flex_instance->transactions_open, 1);
        } else {
            atomic_add(&flex_instance->transactions_open, -1);
        }

        flex_session->transaction_active = router_session->rses_transaction_active;
    }

    return retval;
}

static void diagnostic(FILTER *instance, void *fsession, DCB *dcb) {
}

static void add_waiting_client(FLEXMASTER_FILTER_INSTANCE *flex_instance, DCB *client) {
    DCB *dcb;
    int i = 0;

    while((dcb = flex_instance->waiting_clients[i++]) != NULL) {
        if(dcb == client) {
            return;
        }
    }

    flex_instance->waiting_clients = realloc(flex_instance->waiting_clients, (i + 1) * sizeof(DCB *));
    flex_instance->waiting_clients[i - 1] = client;
    flex_instance->waiting_clients[i] = NULL;
}
