/*
 * This file is distributed as part of MaxScale.  It is free
 * software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation,
 * version 2.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Copyright MariaDB Corporation Ab 2014
 */

/**
 * @file cli.c - A "routing module" that in fact merely gives access
 * to a command line interface
 *
 * @verbatim
 * Revision History
 *
 * Date		Who		Description
 * 18/06/13	Mark Riddoch	Initial implementation
 * 13/06/14	Mark Riddoch	Creted from the debugcli
 *
 * @endverbatim
 */
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

MODULE_INFO info = {
        MODULE_API_ROUTER,
        MODULE_GA,
        ROUTER_VERSION,
        "The flexmaster admin interface"
};

typedef struct {
        SERVICE         *service;
} FLEXMASTER_INSTANCE;

typedef struct {
        SESSION         *session;
} FLEXMASTER_SESSION;

/** Defined in log_manager.cc */
extern int lm_enabled_logfiles_bitmask;
extern size_t log_ses_count[];
extern __thread log_info_t tls_log_info;

static char *version_str = "V1.0.0";

/* The router entry points */
static ROUTER *createInstance(SERVICE *, char **);
static void *newSession(ROUTER *, SESSION *);
static void closeSession(ROUTER *, void *);
static void freeSession(ROUTER *, void *);
static int routeQuery(ROUTER *, void *, GWBUF *);
static void diagnostics(ROUTER *, DCB *);
static void respond_error(FLEXMASTER_SESSION *, int, char *);

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
 * @param service	The service this router is being create for
 * @param options	Any array of options for the query router
 *
 * @return The instance data for this new instance
 */
static ROUTER *createInstance(SERVICE *service, char **options) {
        FLEXMASTER_INSTANCE *instance = (FLEXMASTER_INSTANCE *) malloc(sizeof(FLEXMASTER_INSTANCE));

        if(instance == NULL)
                return NULL;

        instance->service = service;
        return (ROUTER *) instance;
}

/**
 * Associate a new session with this instance of the router.
 *
 * @param instance	The router instance data
 * @param session	The session itself
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
 * @param instance		The router instance data
 * @param router_session	The session being closed
 */
static void closeSession(ROUTER *instance, void *session) {
        free(session);
}

/**
 * Free a debugcli session
 *
 * @param router_instance	The router session
 * @param router_client_session	The router session as returned from newSession
 */
static void freeSession(ROUTER* router_instance, void *router_client_session) {
        return;
}

/**
 * We have data from the client, we must route it to the backend.
 * This is simply a case of sending it to the connection that was
 * chosen when we started the client session.
 *
 * @param instance		The router instance
 * @param router_session	The router session returned from the newSession call
 * @param queue			The queue of data buffers to route
 * @return The number of bytes sent
 */
static int routeQuery(ROUTER *instance, void *session, GWBUF *queue) {
        FLEXMASTER_SESSION *flex_session = (FLEXMASTER_SESSION *) session;
        char *url;

        if ((url = gwbuf_get_property(queue, "URL")) == NULL) {
                respond_error(flex_session, 404, "No URL available");
        }

        gwbuf_free(queue);
        return 0;
}

/**
 * Display router diagnostics
 *
 * @param instance	Instance of the router
 * @param dcb		DCB to send diagnostics to
 */
static void diagnostics(ROUTER *instance, DCB *dcb) {
        return;	/* Nothing to do currently */
}

static void respond_error(FLEXMASTER_SESSION *session, int err, char *msg) {
        DCB *dcb = session->session->client;

        // TODO

        dcb_printf(dcb, "HTTP/1.1 %d %s\n", err, msg);
        dcb_printf(dcb, "Content-Type: text/plain\n");
        dcb_printf(dcb, "\n");
        dcb_printf(dcb, "%d: %s", err, msg);
        dcb_close(dcb);
}