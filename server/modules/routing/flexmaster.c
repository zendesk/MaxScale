#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <service.h>
#include <session.h>
#include <router.h>
#include <modules.h>
#include <modinfo.h>
#include <modutil.h>
#include <atomic.h>
#include <dcb.h>
#include <poll.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <httpd.h>
#include <mysql.h>
#include <flexmaster.h>
#include <readwritesplit.h>

MODULE_INFO info =
{
    MODULE_API_ROUTER,
    MODULE_GA,
    ROUTER_VERSION,
    "The flexmaster admin interface"
};

typedef struct
{
    SERVICE *service;
    FLEXMASTER_FILTER_INSTANCE *filter;
    bool running;
} FLEXMASTER_INSTANCE;

typedef struct
{
    SESSION *session;
} FLEXMASTER_SESSION;

struct flexmaster_parameters
{
    // Set by parse_url_parameters
    char *old_master, *new_master;
    bool rehome, start_slave;

    SERVER *old_master_server, *new_master_server;

    MYSQL *old_master_connection, *new_master_connection;

    // Set in swap, but only used when rehome-ing
    MYSQL_ROW new_master_info;

    // Set in preflight_check_new_master_slave
    // Used in preflight_check_addresses
    char *new_master_master_host;

    FLEXMASTER_INSTANCE *instance;
    char *body;
};

static char *version_str = "V1.0.0";

char *errmsg = NULL;

/* The router entry points */
static ROUTER *createInstance(SERVICE *, char **);
static void *newSession(ROUTER *, SESSION *);
static void closeSession(ROUTER *, void *);
static void freeSession(ROUTER *, void *);
static int routeQuery(ROUTER *, void *, GWBUF *);
static void diagnostics(ROUTER *, DCB *);
static void respond_error(FLEXMASTER_SESSION *, int, char *);

static void master_cut(void *);
static int preflight_check(struct flexmaster_parameters *);
static int lock(FLEXMASTER_INSTANCE *);
static int unlock(FLEXMASTER_INSTANCE *, struct flexmaster_parameters *);
static int swap(struct flexmaster_parameters *);
static int rehome(struct flexmaster_parameters *);
static int start_slave(struct flexmaster_parameters *);

static int preflight_check_addresses(struct flexmaster_parameters *);
static int preflight_check_old_master_rehome(struct flexmaster_parameters *);
static int preflight_check_new_master_slave(struct flexmaster_parameters *);
static int preflight_check_new_master_ro(struct flexmaster_parameters *);
static int preflight_check_old_master_rw(struct flexmaster_parameters *);

static MYSQL *connect_to_mysql(SERVICE *, char *, unsigned short);
static void error(char *);
static void parse_url_parameters(struct flexmaster_parameters *);
static int parse_host_and_port(char *, SERVER **);

/** The module object definition */
static ROUTER_OBJECT MyObject =
{
    createInstance,
    newSession,
    closeSession,
    freeSession,
    routeQuery,
    diagnostics,
    NULL,
    NULL,
    NULL
};

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *version()
{
    return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void ModuleInit()
{
    MXS_INFO("flexmaster router startup %s.\n", version_str);
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
ROUTER_OBJECT *GetModuleObject()
{
    return &MyObject;
}

/**
 * Create an instance of the router for a particular service
 * within the gateway.
 * 
 * @param service The service this router is being create for
 * @param options Any array of options for the query router
 *
 * @return The instance data for this new instance
 */
static ROUTER *createInstance(SERVICE *service, char **options)
{
    if (options == NULL)
    {
        MXS_ERROR("flexmaster: not enough router_options. expected flexmaster_filter");
        return NULL;
    }

    FLEXMASTER_INSTANCE *instance;

    if ((instance = calloc(1, sizeof(FLEXMASTER_INSTANCE))) != NULL)
    {
        instance->service = service;
        instance->running = false;

        FILTER_DEF *filter_def;
        if ((filter_def = filter_find(options[0])) == NULL)
        {
            MXS_ERROR("flexmaster: could not find flexmaster_filter \"%s\"", options[0]);
        }
        else
        {
            // Filter instance is lazy-created on filterApply, so we generally need this
            if (filter_def->obj == NULL)
            {
                filter_def->obj = load_module(filter_def->module, MODULE_FILTER);
            }

            if (filter_def->obj != NULL && filter_def->filter == NULL)
            {
                filter_def->filter = (filter_def->obj->createInstance)(filter_def->options, filter_def->parameters);
            }

            // This will default to NULL if the module and instance creation fail
            instance->filter = (FLEXMASTER_FILTER_INSTANCE *) filter_def->filter;
        }
    }

    return (ROUTER *) instance;
}

/**
 * Associate a new session with this instance of the router.
 *
 * @param instance The router instance data
 * @param session The session itself
 * @return Session specific data for this session
 */
static void *newSession(ROUTER *instance, SESSION *session)
{
    FLEXMASTER_SESSION *flex_session = (FLEXMASTER_SESSION *) malloc(sizeof(FLEXMASTER_SESSION));

    if (flex_session == NULL)
        return NULL;

    flex_session->session = session;
    return flex_session;
}

/**
 * Close a session with the router, this is the mechanism
 * by which a router may cleanup data structure etc.
 *
 * @param instance  The router instance data
 * @param router_session The session being closed
 */
static void closeSession(ROUTER *instance, void *session)
{
    free(session);
}

/**
 * Free a debugcli session
 *
 * @param router_instance The router session
 * @param router_client_session The router session as returned from newSession
 */
static void freeSession(ROUTER* router_instance, void *router_client_session)
{
    return;
}

/**
 * We have data from the client, we must route it to the backend.
 * This is simply a case of sending it to the connection that was
 * chosen when we started the client session.
 *
 * @param instance  The router instance
 * @param router_session The router session returned from the newSession call
 * @param queue   The queue of data buffers to route
 * @return The number of bytes sent
 */
static int routeQuery(ROUTER *instance, void *session, GWBUF *queue)
{
    FLEXMASTER_INSTANCE *flex_instance = (FLEXMASTER_INSTANCE *) instance;
    FLEXMASTER_SESSION *flex_session = session;
    DCB *dcb = flex_session->session->client;
    HTTPD_session *http_session = dcb->data;

    int path = 1 << UF_PATH;

    if ((http_session->url_fields->field_set & path) == path)
    {
        int offset = http_session->url_fields->field_data[UF_PATH].off;
        int len = http_session->url_fields->field_data[UF_PATH].len;

        char *path = http_session->url + offset;

        if (strncmp(path, "/", len) == 0 && http_session->method == HTTP_GET)
        {
            diagnostics(instance, dcb);
        }
        else if (strncmp(path, "/master_cut", len) == 0 && http_session->method == HTTP_POST)
        {
            if (flex_instance->running)
            {
                httpd_respond_error(dcb, 400, "There is already a master cut in progress.");
            }
            else
            {
                struct flexmaster_parameters *params = calloc(1, sizeof(struct flexmaster_parameters));
                // TODO NULL

                params->instance = flex_instance;

                // parse_url_parameters modifies body using strsep
                params->body = malloc(http_session->body_len + 1);
                // TODO error
                strncpy(params->body, http_session->body, http_session->body_len);

                parse_url_parameters(params); // TODO errors

                if (thread_start(NULL, master_cut, params) == NULL)
                {
                    httpd_respond_error(dcb, 400, "There was an error starting a new master cut thread.");
                }
                else
                {
                    // We just throw the thread into the ether, there's nothing we can do
                    flex_instance->running = true;

                    dcb_printf(dcb, "HTTP/1.1 202 Accepted\nConnection: close\n\n");
                    dcb_close(dcb);
                }
            }
        }
        else
        {
            httpd_respond_error(dcb, 404, "Could not find path to route.");
        }
    }
    else
    {
        httpd_respond_error(dcb, 404, "Could not find path to route.");
    }

    return 0;
}

static void master_cut(void *arg)
{
    struct flexmaster_parameters *params = (struct flexmaster_parameters *) arg;
    FLEXMASTER_INSTANCE *flex_instance = params->instance;

    if (params->old_master == NULL || params->new_master == NULL)
    {
        error("Missing required parameters old_master and new_master");
        goto error;
    }

    if (parse_host_and_port(params->old_master, &params->old_master_server) != 0)
    {
        goto error;
    }

    if (parse_host_and_port(params->new_master, &params->new_master_server) != 0)
    {
        goto error;
    }

    if ((params->old_master_connection = connect_to_mysql(flex_instance->service, params->old_master_server->name, params->old_master_server->port)) == NULL)
    {
        goto error;
    }

    if ((params->new_master_connection = connect_to_mysql(flex_instance->service, params->new_master_server->name, params->new_master_server->port)) == NULL)
    {
        goto error;
    }

    if (preflight_check(params) != 0)
    {
        goto error;
    }

    if (lock(flex_instance) != 0)
    {
        goto error;
    }

    if (swap(params) != 0)
    {
        goto error;
    }

    if (params->rehome)
    {
        if (rehome(params) != 0)
        {
            goto error;
        }

        if (params->start_slave)
        {
            if (start_slave(params) != 0)
            {
                goto error;
            }
        }
    }

    if (unlock(flex_instance, params) != 0)
    {
        goto error;
    }

    // don't fall through
    goto free;

error:
    MXS_ERROR("%s", errmsg);
    free(errmsg);

free:
    if (params->new_master_master_host != NULL)
    {
        free(params->new_master_master_host);
    }

    if (params->old_master_connection != NULL)
    {
        mysql_close(params->old_master_connection);
    }

    if (params->new_master_connection != NULL)
    {
        mysql_close(params->new_master_connection);
    }

    free(params->body);
    free(params);
}

static int lock(FLEXMASTER_INSTANCE *instance)
{
    if (instance->filter != NULL)
    {
        spinlock_acquire(&instance->filter->transaction_lock);

        // spinlock on the num of open transactions
        // TODO timeout?
        while (instance->filter->transactions_open > 0);
    }

    return 0;
}

static int unlock(FLEXMASTER_INSTANCE *instance, struct flexmaster_parameters *params)
{
    // spinlock on new master status
    // TODO timeout?
    while (!SERVER_IS_MASTER(params->new_master_server));

    if (instance->filter != NULL)
    {
        DCB *dcb;
        int i = 0;

        // Lock the session?
        // while ((dcb = instance->filter->waiting_clients[i++]) != NULL) {
        //}

        spinlock_release(&instance->filter->transaction_lock);

        i = 0;
        while ((dcb = instance->filter->waiting_clients[i++]) != NULL)
        {
            ROUTER_CLIENT_SES *router_session = dcb->session->router_session;
            spinlock_acquire(&router_session->rses_lock);

            if (router_session->rses_master_ref != NULL)
            {
                // Hack for readwritesplit master failover, we know what the master
                // should be, we just need the bref_backend_ref for it
                int j;
                for (j = 0; j < router_session->rses_nbackends; j++)
                {
                    backend_ref_t* bref = &router_session->rses_backend_ref[j];

                    if (bref->bref_backend->backend_server == params->new_master_server)
                    {
                        router_session->rses_master_ref = bref;
                        break;
                    }
                }
            }

            spinlock_release(&router_session->rses_lock);

            // vaguely copied from readwritesplit
            // run through each full packet and route it
            // (client would have to implement async for multiple queries?)
            GWBUF *querybuf, *tmpbuf;
            querybuf = tmpbuf = dcb->dcb_readqueue;

            while (tmpbuf != NULL)
            {
                if ((querybuf = modutil_get_next_MySQL_packet(&tmpbuf)) == NULL)
                {
                    if (GWBUF_LENGTH(tmpbuf) > 0) { // TODO?
                        dcb->dcb_readqueue = gwbuf_append(dcb->dcb_readqueue, tmpbuf);
                        break;
                    }
                }

                // go through the full filter chain too
                DOWNSTREAM head = dcb->session->head;
                head.routeQuery(head.instance, head.session, querybuf);
            }
        }
    }

    return 0;
}

static int swap(struct flexmaster_parameters *params)
{
    // Set the old master RO
    if (mysql_query(params->old_master_connection, "SET GLOBAL READ_ONLY=1") != 0)
    {
        error("Error: could not set old master read only");
        return 1;
    }

    // Stop replication on the new master
    if (mysql_query(params->new_master_connection, "STOP SLAVE") != 0)
    {
        error("Error: could not stop slave on new master");
        return 1;
    }

    /*
     * Query old master status.
     * The information is only used if the rehome option is set,
     * but it also serves as a final check before setting the new master read-write.
     * TODO: confirm this can't be moved
     */
    if (mysql_query(params->new_master_connection, "SHOW MASTER STATUS") != 0)
    {
        error("Error: could not query old master status");
        return 1;
    }

    MYSQL_RES *result = mysql_store_result(params->new_master_connection);

    if (result == NULL)
    {
        error("Error: could not query old master status");
        return 1;
    }

    params->new_master_info = mysql_fetch_row(result);
    mysql_free_result(result);

    if (params->new_master_info == NULL)
    {
        error("Error: could not query old master status");
        return 1;
    }

    // Set the new master RW
    if (mysql_query(params->new_master_connection, "SET GLOBAL READ_ONLY=0") != 0)
    {
        error("Error: could not set new master not readonly");
        return 1;
    }

    return 0;
}

static int rehome(struct flexmaster_parameters *params)
{
    char *master_log_file = params->new_master_info[0];
    unsigned int master_log_pos = strtol(params->new_master_info[1], NULL, 0);

    char query[1024];

    int res = snprintf((char *) &query, 1023,
            "CHANGE MASTER to master_host='%s', master_port=%d, master_log_file='%s', master_log_pos=%d",
            params->new_master_server->name, params->new_master_server->port, master_log_file, master_log_pos
            );

    if (res == 1023 || res < 0)
    {
        error("Error: could not compose change master query");
        return 1;
    }

    if (mysql_query(params->old_master_connection, query) != 0)
    {
        error("Error: could not change master");
        return 1;
    }

    return 0;
}

static int start_slave(struct flexmaster_parameters *params)
{
    if (mysql_query(params->old_master_connection, "START SLAVE") != 0)
    {
        error("Error: could not start slave on old master");
        return 1;
    }

    return 0;
}

static int preflight_check(struct flexmaster_parameters *params)
{
    // NB: order is important.
    // check_addresses must come after new_master_slave check
    if (preflight_check_old_master_rehome(params) != 0 ||
            preflight_check_new_master_slave(params) != 0 ||
            preflight_check_new_master_ro(params) != 0 ||
            preflight_check_old_master_rw(params) != 0 ||
            preflight_check_addresses(params) != 0)

    {

        return 1;
    }

    return 0;
}

static int preflight_check_old_master_rw(struct flexmaster_parameters *params)
{
    // old master is not readonly
    if (mysql_query(params->old_master_connection, "SELECT @@read_only") != 0)
    {
        error("Error: could not query old master @@read_only");
        return 1;
    }

    MYSQL_RES *result = mysql_store_result(params->old_master_connection);

    if (result == NULL)
    {
        error("Error: could not query old master @@read_only");
        return 1;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    mysql_free_result(result);

    if (row == NULL)
    {
        error("Error: could not query old master @@read_only");
        return 1;
    }

    if (strcmp(row[0], "0") != 0)
    {
        error("Error: old master is read only!");
        return 1;
    }

    return 0;
}

static int preflight_check_new_master_ro(struct flexmaster_parameters *params)
{
    // new master is readonly
    if (mysql_query(params->new_master_connection, "SELECT @@read_only") != 0)
    {
        error("Error: could not query new master @@read_only");
        return 1;
    }

    MYSQL_RES *result = mysql_store_result(params->new_master_connection);

    if (result == NULL)
    {
        error("Error: could not query new master @@read_only");
        return 1;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    mysql_free_result(result);

    if (row == NULL)
    {
        error("Error: could not query new master @@read_only");
        return 1;
    }

    if (strcmp(row[0], "1") != 0)
    {
        error("Error: new master is not read only!");
        return 1;
    }

    return 0;
}

static int preflight_check_new_master_slave(struct flexmaster_parameters *params)
{
    // new master is (a slave, running (IO, SQL), not delayed)
    if (mysql_query(params->new_master_connection, "SHOW SLAVE STATUS") != 0)
    {
        error("Error: could not query new master slave status");
        return 1;
    }

    MYSQL_RES *result = mysql_store_result(params->new_master_connection);

    if (result == NULL)
    {
        error("Error: could not query new master slave status");
        return 1;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    mysql_free_result(result);

    if (row == NULL)
    {
        error("Error: could not query new master slave status");
        return 1;
    }

    char *slave_io = row[10];
    char *slave_sql = row[11];
    char *seconds_behind = row[32];

    // Used by preflight_check_addresses
    params->new_master_master_host = malloc(strlen(row[1]) + 1);

    if (params->new_master_master_host == NULL)
    {
        error("Error: could not alloc new_master_master_host");
        return 1;
    }

    strcpy(params->new_master_master_host, row[1]);

    if (strcmp(slave_io, "Yes") != 0)
    {
        error("Error: new master IO is not running");
        return 1;
    }

    if (strcmp(slave_sql, "Yes") != 0)
    {
        error("Error: new master SQL is not running");
        return 1;
    }

    if (strcmp(seconds_behind, "0") != 0)
    {
        error("Error: new master is not up-to-date");
        return 1;
    }

    return 0;
}

static int preflight_check_old_master_rehome(struct flexmaster_parameters *params)
{
    if (!params->rehome)
    {
        return 0;
    }

    // old master has proper credentials for rehome
    if (mysql_query(params->old_master_connection, "SHOW SLAVE STATUS") != 0)
    {
        error("Error: could not query old master slave status");
        return 1;
    }

    MYSQL_RES *result = mysql_store_result(params->old_master_connection);

    if (result == NULL)
    {
        error("Error: could not query old master slave status");
        return 1;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    mysql_free_result(result);

    if (row == NULL)
    {
        error("Error: could not query old master slave status");
        return 1;
    }

    char *master_user = row[2];

    if (strlen(master_user) == 0 || strcmp(master_user, "test") == 0)
    {
        error("Error: old master does not have proper credentials, cannot rehome");
        return 1;
    }

    return 0;
}

static int preflight_check_addresses(struct flexmaster_parameters *params)
{
    // slave master ip = master ip
    struct addrinfo *old_master_addrinfo, *new_master_addrinfo;

    if (getaddrinfo(params->old_master_server->name, NULL, NULL, &old_master_addrinfo) != 0)
    {
        error("Error: could not obtain IP for old master address");
        return 1;
    }

    if (getaddrinfo(params->new_master_master_host, NULL, NULL, &new_master_addrinfo) != 0)
    {
        error("Error: could not obtain IP for new master's Master_Host address");
        return 1;
    }

    if (strcmp(old_master_addrinfo->ai_addr->sa_data, new_master_addrinfo->ai_addr->sa_data) != 0)
    {
        error("Error: new master is not a slave to the old master!");
        return 1;
    }

    free(old_master_addrinfo);
    free(new_master_addrinfo);

    return 0;
}

static void error(char *msg)
{
    errmsg = malloc(strlen(msg) + 1);
    strcpy(errmsg, msg);
}

static MYSQL *connect_to_mysql(SERVICE *service, char *host, unsigned short port)
{
    MYSQL *connection = mysql_init(NULL);

    if (connection == NULL)
    {
        error("Error: could not initialize mysql connection");
        return NULL;
    }

    int timeout = 3;

    if (mysql_options(connection, MYSQL_OPT_READ_TIMEOUT, (void *) &timeout) != 0)
    {
        error("Error: failed to set read timeout value for backend connection.");
        mysql_close(connection);
        return NULL;
    }

    if (mysql_options(connection, MYSQL_OPT_CONNECT_TIMEOUT, (void *) &timeout) != 0)
    {
        error("Error: failed to set connect timeout value for backend connection.");
        mysql_close(connection);
        return NULL;
    }

    if (mysql_options(connection, MYSQL_OPT_WRITE_TIMEOUT, (void *) &timeout) != 0)
    {
        error("Error: failed to set connect timeout value for backend connection.");
        mysql_close(connection);
        return NULL;
    }

    if (mysql_options(connection, MYSQL_OPT_USE_REMOTE_CONNECTION, NULL) != 0)
    {
        error("Error: failed to set external connection. It is needed for backend server connections.");
        mysql_close(connection);
        return NULL;
    }

    if (mysql_real_connect(connection, host, service->credentials.name, service->credentials.authdata, NULL, port, NULL, 0) == NULL)
    {
        error("Error: could not connect to server.");
        mysql_close(connection);
        return NULL;
    }

    return connection;
}

static void parse_url_parameters(struct flexmaster_parameters *params)
{
    char *token, *field, *value;

    while ((token = strsep(&params->body, "&")) != NULL)
    {
        field = strsep(&token, "=");
        value = token;

        if (strcmp(field, "old_master") == 0)
        {
            params->old_master = value;
        }
        else if (strcmp(field, "new_master") == 0)
        {
            params->new_master = value;
        }
        else if (strcmp(field, "rehome") == 0)
        {
            params->rehome = true;
        }
        else if (strcmp(field, "start_slave") == 0)
        {
            params->start_slave = true;
        }
    }
}

static int parse_host_and_port(char *str, SERVER **server)
{
    char *host = strsep(&str, ":");

    if (host == NULL || str == NULL)
    {
        error("Error: could not grab host and port from address");
        return 1;
    }

    unsigned short port = strtoul(str, NULL, 0);

    if (port == 0)
    {
        error("Error: could not grab port from address");
        return 1;
    }

    *server = server_find(host, port);

    if (*server == NULL)
    {
        error("Error: could not find maxscale server from address");
        return 1;
    }

    return 0;
}

/**
 * Display router diagnostics
 *
 * @param instance Instance of the router
 * @param dcb DCB to send diagnostics to
 */
static void diagnostics(ROUTER *instance, DCB *dcb)
{
    char date[64] = "";
    const char *fmt = "%a, %d %b %Y %H:%M:%S GMT";
    time_t httpd_current_time = time(NULL);

    strftime(date, sizeof(date), fmt, localtime(&httpd_current_time));
    dcb_printf(dcb, "HTTP/1.1 200 OK\nDate: %s\nConnection: close\n\n", date);
    dcb_close(dcb);
}
