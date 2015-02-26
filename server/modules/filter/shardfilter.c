#include <stdio.h>
#include <filter.h>
#include <mysql.h>
#include <mysql_client_server_protocol.h>
#include <buffer.h>
#include <modutil.h>

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

static FILTER *createInstance(char **options, FILTER_PARAMETER **params);
static void *newSession(FILTER *instance, SESSION *session);
static void closeSession(FILTER *instance, void *session);
static void freeSession(FILTER *instance, void *session);
static void setDownstream(FILTER *instance, void *fsession, DOWNSTREAM *downstream);
static int routeQuery(FILTER *instance, void *fsession, GWBUF *queue);
static void diagnostic(FILTER *instance, void *fsession, DCB *dcb);

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
} ZENDESK_INSTANCE;

typedef struct {
        DOWNSTREAM shard_server;
        SESSION *rses;
} ZENDESK_SESSION;

int accountMap[][2] = {
        {1, 2},
        {2, 5}
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

	if ((my_instance = calloc(1, sizeof(ZENDESK_INSTANCE))) != NULL)
		my_instance->sessions = 0;

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
        ZENDESK_INSTANCE *my_instance = (ZENDESK_INSTANCE *) instance;
        ZENDESK_SESSION *my_session;

	if ((my_session = calloc(1, sizeof(ZENDESK_SESSION))) != NULL) {
                my_instance->sessions++;
                my_session->rses = session;
	}

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
        return;
}

/**
 * Set the downstream component for this filter.
 *
 * @param instance	The filter instance data
 * @param session	The session being closed
 * @param downstream	The downstream filter or router
 */
static void setDownstream(FILTER *instance, void *session, DOWNSTREAM *downstream) {
        ZENDESK_SESSION *my_session = (ZENDESK_SESSION *) session;
	my_session->shard_server = *downstream;
}

/**
 * The routeQuery entry point. This is passed the query buffer
 * to which the filter should be applied. Once applied the
 * query shoudl normally be passed to the downstream component
 * (filter or router) in the filter chain.
 *
 * @param instance	The filter instance data
 * @param session	The filter session
 * @param queue		The query data
 */
static int routeQuery(FILTER *instance, void *session, GWBUF *queue) {
        ZENDESK_SESSION *my_session = (ZENDESK_SESSION *) session;

#if 0
        if(MYSQL_GET_PACKET_NO(queue->data) == MYSQL_COM_INIT_DB) {
                /**
                 * Record database name and store to session.
                 */
                GWBUF* tmpbuf;
                MYSQL_session* data;
                unsigned int qlen;

                data = scur->data;
                memset(data->db, 0, MYSQL_DATABASE_MAXLEN+1);

                tmpbuf = scur->scmd_cur_cmd->my_sescmd_buf;
                qlen = MYSQL_GET_PACKET_LEN((unsigned char*)tmpbuf->start);

                if(qlen > 0 && qlen < MYSQL_DATABASE_MAXLEN+1) {
                        char *database_name = tmpbuf->start + 5;

                        if(strncmp("account_", database_name, 8) == 0) {
                                // grab the id of the account
                                int account_id = strtol(database_name + 8, NULL, 0);

                                if(account_id > 0) {
                                        int *map = accountMap[account_id];

                                        if(map) {
                                                int shard_id = map[0];

                                                char shard_database_id[255];
                                                snprintf((char *) &shard_database_id, 255, "shard_%d", shard_id);

                                                // XXX: modutil_replace_SQL checks explicitly for COM_QUERY
                                                // but just generically replaces the GWBUF data
                                                ((char *) tmpbuf->start)[4] = MYSQL_COM_QUERY;

                                                tmpbuf = modutil_replace_SQL(tmpbuf, shard_database_id);
                                                scur->scmd_cur_cmd->my_sescmd_buf = gwbuf_make_contiguous(tmpbuf);

                                                ((char *) scur->scmd_cur_cmd->my_sescmd_buf->start)[4] = MYSQL_COM_INIT_DB;

                                                // If the tmpbuf is already contiguous, gwbuf_make_contiguous
                                                // returns the original pointer. Otherwise it returns a newly allocated one.
                                                if(scur->scmd_cur_cmd->my_sescmd_buf != tmpbuf)
                                                        gwbuf_free(tmpbuf);

                                                tmpbuf = scur->scmd_cur_cmd->my_sescmd_buf;
                                                qlen = strlen(shard_database_id);
                                        }
                                }
                        }

                        strncpy(data->db, tmpbuf->start+5, qlen - 1);
                }
        }
#endif

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
static void diagnostic(FILTER *instance, void *fsession, DCB *dcb) {
        // ZENDESK_INSTANCE *my_instance = (ZENDESK_INSTANCE *) instance;
        // ZENDESK_SESSION *my_session = (ZENDESK_SESSION *) fsession;
}
