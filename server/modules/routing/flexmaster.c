#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <service.h>
#include <session.h>
#include <router.h>
#include <modules.h>
#include <modinfo.h>
#include <atomic.h>
#include <spinlock.h>
#include <dcb.h>
#include <poll.h>
#include <skygw_utils.h>
#include <log_manager.h>
#include <httpd.h>
#include <mysql.h>
#include <stdbool.h>

MODULE_INFO info = {
        MODULE_API_ROUTER,
        MODULE_GA,
        ROUTER_VERSION,
        "The flexmaster admin interface"
};

typedef struct {
        SERVICE *service;
        SERVICE *top;
} FLEXMASTER_INSTANCE;

typedef struct {
        SESSION *session;
} FLEXMASTER_SESSION;

struct flexmaster_parameters {
        // Set by parse_url_parameters
        char *old_master, *new_master;
        bool rehome, start_slave;

        char *old_master_host, *new_master_host;
        unsigned int old_master_port, new_master_port;

        MYSQL *old_master_connection, *new_master_connection;

        // Set in swap, but only used when rehome-ing
        MYSQL_ROW new_master_info;

        // Set in preflight_check_new_master_slave
        // Used in preflight_check_addresses
        char *new_master_master_host;
};

/** Defined in log_manager.cc */
extern int lm_enabled_logfiles_bitmask;
extern size_t log_ses_count[];
extern __thread log_info_t tls_log_info;

static char *version_str = "V1.0.0";

char *errmsg;

/* The router entry points */
static ROUTER *createInstance(SERVICE *, char **);
static void *newSession(ROUTER *, SESSION *);
static void closeSession(ROUTER *, void *);
static void freeSession(ROUTER *, void *);
static int routeQuery(ROUTER *, void *, GWBUF *);
static void diagnostics(ROUTER *, DCB *);
static void respond_error(FLEXMASTER_SESSION *, int, char *);

static void master_cut(FLEXMASTER_INSTANCE *, DCB *, HTTPD_session *);
static int preflight_check(struct flexmaster_parameters *);
static int swap(struct flexmaster_parameters *);
static int rehome(struct flexmaster_parameters *);
static int start_slave(struct flexmaster_parameters *);

static int preflight_check_addresses(struct flexmaster_parameters *);
static int preflight_check_old_master_rehome(struct flexmaster_parameters *);
static int preflight_check_new_master_slave(struct flexmaster_parameters *);
static int preflight_check_new_master_ro(struct flexmaster_parameters *);
static int preflight_check_old_master_rw(struct flexmaster_parameters *);

static MYSQL *mysql_connect(SERVICE *, char *, unsigned int);
static void error(char *);
static void parse_url_parameters(char *, struct flexmaster_parameters *);
static int parse_host_and_port(char *, char **, unsigned int *);

/** The module object definition */
static ROUTER_OBJECT MyObject = {
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
char *version() {
        return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void ModuleInit() {
        LOGIF(LM, (skygw_log_write(LOGFILE_MESSAGE, "flexmaster router startup %s.\n", version_str)));
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
ROUTER_OBJECT *GetModuleObject() {
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
static ROUTER *createInstance(SERVICE *service, char **options) {
        FLEXMASTER_INSTANCE *instance = malloc(sizeof(FLEXMASTER_INSTANCE));

        if(instance == NULL)
                return NULL;

        instance->service = service;

        return (ROUTER *) instance;
}

/**
 * Associate a new session with this instance of the router.
 *
 * @param instance The router instance data
 * @param session The session itself
 * @return Session specific data for this session
 */
static void *newSession(ROUTER *instance, SESSION *session) {
        FLEXMASTER_SESSION *flex_session = (FLEXMASTER_SESSION *) malloc(sizeof(FLEXMASTER_SESSION));

        if(flex_session == NULL)
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
static void closeSession(ROUTER *instance, void *session) {
        free(session);
}

/**
 * Free a debugcli session
 *
 * @param router_instance The router session
 * @param router_client_session The router session as returned from newSession
 */
static void freeSession(ROUTER* router_instance, void *router_client_session) {
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
static int routeQuery(ROUTER *instance, void *session, GWBUF *queue) {
        FLEXMASTER_INSTANCE *flex_instance = (FLEXMASTER_INSTANCE *) instance;
        FLEXMASTER_SESSION *flex_session = session;
        DCB *dcb = flex_session->session->client;
        HTTPD_session *http_session = dcb->data;

        int path = 1 << UF_PATH;

        if((http_session->url_fields->field_set & path) == path) {
                int offset = http_session->url_fields->field_data[UF_PATH].off;
                int len = http_session->url_fields->field_data[UF_PATH].len;

                char *path = http_session->url + offset;

                if(strncmp(path, "/", len) == 0 && http_session->method == HTTP_GET) {
                        diagnostics(instance, dcb);
                } else if(strncmp(path, "/master_cut", len) == 0 && http_session->method == HTTP_POST) {
                        master_cut(flex_instance, dcb, http_session);
                } else {
                        httpd_respond_error(dcb, 404, "Could not find path to route.");
                }
        } else {
                httpd_respond_error(dcb, 404, "Could not find path to route.");
        }

        return 0;
}

static void master_cut(FLEXMASTER_INSTANCE *flex_instance, DCB *dcb, HTTPD_session *http_session) {
        struct flexmaster_parameters params = {0};

        // parse_url_parameters modifies body using strsep
        char *body = malloc(http_session->body_len + 1);
        strncpy(body, http_session->body, http_session->body_len);
        parse_url_parameters(body, &params);

        if(params.old_master == NULL || params.new_master == NULL) {
                httpd_respond_error(dcb, 400, "Missing required parameters old_master and new_master");
                return;
        }

        if(parse_host_and_port(params.old_master, &params.old_master_host, &params.old_master_port) != 0) {
                goto error;
        }

        if(parse_host_and_port(params.new_master, &params.new_master_host, &params.new_master_port) != 0) {
                goto error;
        }

        if((params.old_master_connection = mysql_connect(flex_instance->service, params.old_master_host, params.old_master_port)) == NULL) {
                goto error;
        }

        if((params.new_master_connection = mysql_connect(flex_instance->service, params.new_master_host, params.new_master_port)) == NULL) {
                goto error;
        }

        if(preflight_check(&params) != 0) {
                goto error;
        }

        if(swap(&params) != 0) {
                goto error;
        }

        if(params.rehome) {
                if(rehome(&params) != 0) {
                        goto error;
                }
        }

        if(params.start_slave) {
                if(start_slave(&params) != 0) {
                        goto error;
                }
        }

        dcb_printf(dcb, "HTTP/1.1 200 OK\nConnection: close\n\n");
        dcb_close(dcb);

        // don't fall through
        goto free;

error:
        httpd_respond_error(dcb, 400, errmsg);

free:
        free(body);

        if(params.new_master_master_host != NULL) {
                free(params.new_master_master_host);
        }

        if(params.old_master_connection) {
                mysql_close(params.old_master_connection);
        }

        if(params.new_master_connection) {
                mysql_close(params.new_master_connection);
        }

}

static int swap(struct flexmaster_parameters *params) {
        // Set the old master RO
        if(mysql_query(params->old_master_connection, "SET GLOBAL READ_ONLY=1") != 0) {
                error("Error: could not set old master read only");
                return 1;
        }

        // Stop replication on the new master
        if(mysql_query(params->new_master_connection, "STOP SLAVE") != 0) {
                error("Error: could not stop slave on new master");
                return 1;
        }

        /*
         * Query old master status.
         * The information is only used if the rehome option is set,
         * but it also serves as a final check before setting the new master read-write.
         * TODO: confirm this can't be moved
         */
        if(mysql_query(params->new_master_connection, "SHOW MASTER STATUS") != 0) {
                error("Error: could not query old master status");
                return 1;
        }

        MYSQL_RES *result = mysql_store_result(params->new_master_connection);

        if(result == NULL) {
                error("Error: could not query old master status");
                return 1;
        }

        params->new_master_info = mysql_fetch_row(result);
        mysql_free_result(result);

        if(params->new_master_info == NULL) {
                error("Error: could not query old master status");
                return 1;
        }

        // Set the new master RW
        if(mysql_query(params->new_master_connection, "SET GLOBAL READ_ONLY=0") != 0) {
                error("Error: could not set new master not readonly");
                return 1;
        }

        return 0;
}

static int rehome(struct flexmaster_parameters *params) {
        char *master_log_file = params->new_master_info[0];
        unsigned int master_log_pos = strtol(params->new_master_info[1], NULL, 0);

        char query[1024];

        int res = snprintf((char *) &query, 1023,
                "CHANGE MASTER to master_host='%s', master_port=%d, master_log_file='%s', master_log_pos=%d",
                params->new_master_host, params->new_master_port, master_log_file, master_log_pos
        );

        if(res == 1023 || res < 0) {
                error("Error: could not compose change master query");
                return 1;
        }

        if(mysql_query(params->old_master_connection, query) != 0) {
                error("Error: could not change master");
                return 1;
        }

        return 0;
}

static int start_slave(struct flexmaster_parameters *params) {
        if(mysql_query(params->old_master_connection, "START SLAVE") != 0) {
                error("Error: could not start slave on old master");
                return 1;
        }

        return 0;
}

static int preflight_check(struct flexmaster_parameters *params) {
        // NB: order is important.
        // check_addresses must come after new_master_slave check
        if(preflight_check_old_master_rehome(params) != 0 ||
                        preflight_check_new_master_slave(params) != 0 ||
                        preflight_check_new_master_ro(params) != 0 ||
                        preflight_check_old_master_rw(params) != 0 ||
                        preflight_check_addresses(params) != 0)
        {

                return 1;
        }

        return 0;
}

static int preflight_check_old_master_rw(struct flexmaster_parameters *params) {
        // old master is not readonly
        if(mysql_query(params->old_master_connection, "SELECT @@read_only") != 0) {
                error("Error: could not query old master @@read_only");
                return 1;
        }

        MYSQL_RES *result = mysql_store_result(params->old_master_connection);

        if(result == NULL) {
                error("Error: could not query old master @@read_only");
                return 1;
        }

        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);

        if(row == NULL) {
                error("Error: could not query old master @@read_only");
                return 1;
        }

        if(strcmp(row[0], "0") != 0) {
                error("Error: old master is read only!");
                return 1;
        }

        return 0;
}

static int preflight_check_new_master_ro(struct flexmaster_parameters *params) {
        // new master is readonly
        if(mysql_query(params->new_master_connection, "SELECT @@read_only") != 0) {
                error("Error: could not query new master @@read_only");
                return 1;
        }

        MYSQL_RES *result = mysql_store_result(params->new_master_connection);

        if(result == NULL) {
                error("Error: could not query new master @@read_only");
                return 1;
        }

        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);

        if(row == NULL) {
                error("Error: could not query new master @@read_only");
                return 1;
        }

        if(strcmp(row[0], "1") != 0) {
                error("Error: new master is not read only!");
                return 1;
        }

        return 0;
}

static int preflight_check_new_master_slave(struct flexmaster_parameters *params) {
        // new master is (a slave, running (IO, SQL), not delayed)
        if(mysql_query(params->new_master_connection, "SHOW SLAVE STATUS") != 0) {
                error("Error: could not query new master slave status");
                return 1;
        }

        MYSQL_RES *result = mysql_store_result(params->new_master_connection);

        if(result == NULL) {
                error("Error: could not query new master slave status");
                return 1;
        }

        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);

        if(row == NULL) {
                error("Error: could not query new master slave status");
                return 1;
        }

        char *slave_io = row[10];
        char *slave_sql = row[11];
        char *seconds_behind = row[32];

        // Used by preflight_check_addresses
        params->new_master_master_host = malloc(strlen(row[1]) + 1);

        if(params->new_master_master_host == NULL) {
                error("Error: could not alloc new_master_master_host");
                return 1;
        }

        strcpy(params->new_master_master_host, row[1]);

        if(strcmp(slave_io, "Yes") != 0) {
                error("Error: new master IO is not running");
                return 1;
        }

        if(strcmp(slave_sql, "Yes") != 0) {
                error("Error: new master SQL is not running");
                return 1;
        }

        if(strcmp(seconds_behind, "0") != 0) {
                error("Error: new master is not up-to-date");
                return 1;
        }

        return 0;
}

static int preflight_check_old_master_rehome(struct flexmaster_parameters *params) {
        if(!params->rehome) {
                return 0;
        }

        // old master has proper credentials for rehome
        if(mysql_query(params->old_master_connection, "SHOW SLAVE STATUS") != 0) {
                error("Error: could not query old master slave status");
                return 1;
        }

        MYSQL_RES *result = mysql_store_result(params->old_master_connection);

        if(result == NULL) {
                error("Error: could not query old master slave status");
                return 1;
        }

        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);

        if(row == NULL) {
                error("Error: could not query old master slave status");
                return 1;
        }

        char *master_user = row[2];

        if(strlen(master_user) == 0 || strcmp(master_user, "test") == 0) {
                error("Error: old master does not have proper credentials, cannot rehome");
                return 1;
        }

        return 0;
}

static int preflight_check_addresses(struct flexmaster_parameters *params) {
        // slave master ip = master ip
        struct addrinfo *old_master_addrinfo, *new_master_addrinfo;

        if(getaddrinfo(params->old_master_host, NULL, NULL, &old_master_addrinfo) != 0) {
                error("Error: could not obtain IP for old master address");
                return 1;
        }

        if(getaddrinfo(params->new_master_master_host, NULL, NULL, &new_master_addrinfo) != 0) {
                error("Error: could not obtain IP for new master's Master_Host address");
                return 1;
        }

        if(strcmp(old_master_addrinfo->ai_addr->sa_data, new_master_addrinfo->ai_addr->sa_data) != 0) {
                error("Error: new master is not a slave to the old master!");
                return 1;
        }

        free(old_master_addrinfo);
        free(new_master_addrinfo);

        return 0;
}

static void error(char *msg) {
        LOGIF(LE, (skygw_log_write_flush(LOGFILE_ERROR, msg)));
        errmsg = malloc(strlen(msg) + 1);
        strcpy(errmsg, msg);
}

static MYSQL *mysql_connect(SERVICE *service, char *host, unsigned int port) {
        MYSQL *connection = mysql_init(NULL);

        if(connection == NULL) {
                error("Error: could not initialize mysql connection");
                return NULL;
        }

        int timeout = 3;

        if(mysql_options(connection, MYSQL_OPT_READ_TIMEOUT, (void *) &timeout) != 0) {
                error("Error: failed to set read timeout value for backend connection.");
                mysql_close(connection);
                return NULL;
        }

        if(mysql_options(connection, MYSQL_OPT_CONNECT_TIMEOUT, (void *) &timeout) != 0) {
                error("Error: failed to set connect timeout value for backend connection.");
                mysql_close(connection);
                return NULL;
        }

        if(mysql_options(connection, MYSQL_OPT_WRITE_TIMEOUT, (void *) &timeout) != 0) {
                error("Error: failed to set connect timeout value for backend connection.");
                mysql_close(connection);
                return NULL;
        }

        if(mysql_options(connection, MYSQL_OPT_USE_REMOTE_CONNECTION, NULL) != 0) {
                error("Error: failed to set external connection. It is needed for backend server connections.");
                mysql_close(connection);
                return NULL;
        }

        if(mysql_real_connect(connection, host, service->credentials.name, service->credentials.authdata, NULL, port, NULL, 0) == NULL) {
                error("Error: could not connect to server.");
                mysql_close(connection);
                return NULL;
        }

        return connection;
}

static void parse_url_parameters(char *body, struct flexmaster_parameters *params) {
        char *token, *field, *value;

        while((token = strsep(&body, "&")) != NULL) {
                field = strsep(&token, "=");
                value = token;

                if(strcmp(field, "old_master") == 0) {
                        params->old_master = value;
                } else if(strcmp(field, "new_master") == 0) {
                        params->new_master = value;
                } else if(strcmp(field, "rehome") == 0) {
                        params->rehome = true;
                } else if(strcmp(field, "start_slave") == 0) {
                        params->start_slave = true;
                }
        }
}

static int parse_host_and_port(char *str, char **host, unsigned int *port) {
        *host = strsep(&str, ":");

        if(host == NULL || str == NULL) {
                error("Error: could not grab host and port from old master address");
                return 1;
        }

        *port = strtol(str, NULL, 0);

        if(*port == 0) {
                error("Error: could not grab port from old master address");
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
static void diagnostics(ROUTER *instance, DCB *dcb) {
        char date[64] = "";
        const char *fmt = "%a, %d %b %Y %H:%M:%S GMT";
        time_t httpd_current_time = time(NULL);

        strftime(date, sizeof(date), fmt, localtime(&httpd_current_time));
        dcb_printf(dcb, "HTTP/1.1 200 OK\nDate: %s\nConnection: close\n\n", date);
        dcb_close(dcb);
}
