#include <stdio.h>
#include <router.h>
#include <modinfo.h>

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

typedef struct{
}TESTROUTER;

typedef struct{
}TESTSESSION;

char *version() {
        return version_str;
}

void ModuleInit() {
}

ROUTER_OBJECT *GetModuleObject() {
        return &MyObject;
}

static ROUTER *createInstance(SERVICE *service, char **options) {
        return (ROUTER *) malloc(sizeof(TESTROUTER));
}

static void *newSession(ROUTER *instance, SESSION *session) {
        return (SESSION *) malloc(sizeof(TESTSESSION));
}

static void closeSession(ROUTER *instance, void *session) {
}

static void freeSession(ROUTER *router_instance, void *router_client_session) {
        free(router_client_session);
}

static int routeQuery(ROUTER *instance, void *session, GWBUF *queue) {
        return 0;
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
