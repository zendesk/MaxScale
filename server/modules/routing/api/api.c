/*
 * This file is distributed as part of the SkySQL Gateway.  It is free
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
 * Copyright SkySQL Ab 2013
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

typedef struct {
	SERVICE		*service;
} WEB_INSTANCE;

typedef struct {
	SESSION		*session;
} WEB_SESSION;

#include <inttypes.h>
#include <sapi/embed/php_embed.h>

#ifdef ZTS
    void ***tsrm_ls;
#endif

    PHP_FUNCTION(send_api_results) {
        char *php_session;
        char *php_response;
        int php_session_len, php_response_len;
        long sessionint;
        WEB_SESSION *websession;
        DCB *dcb;
        if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &php_session,  &php_session_len, &php_response, &php_response_len) == FAILURE) {
            RETURN_NULL();
        }
        sscanf(php_session, "%ld", &sessionint);
        websession = (WEB_SESSION *) sessionint;
        dcb = websession->session->client;
        dcb_printf(dcb, "In api.c before response");
        dcb_printf(dcb, php_response);
        dcb_printf(dcb, "In api.c after response");
        dcb_close(dcb);
    }
    
    static zend_function_entry maxapi_functions[] = {
        PHP_FE(send_api_results, NULL)
        {   NULL, NULL, NULL    }
    };
    
/* Extension bits */
    zend_module_entry php_mymod_module_entry = {
        STANDARD_MODULE_HEADER,
        "maxapi",   /* extension name */
        maxapi_functions,       /* function entries */
        NULL,       /* MINIT */
        NULL,       /* MSHUTDOWN */
        NULL,       /* RINIT */
        NULL,       /* RSHUTDOWN */
        NULL,       /* MINFO */
        "1.0",      /* Version */
        STANDARD_MODULE_PROPERTIES
    };    
    
static char *version_str = "V1.0.0";

MODULE_INFO 	info = {
	MODULE_API_ROUTER,
	MODULE_IN_DEVELOPMENT,
	ROUTER_VERSION,
	"The API router"
};

static	ROUTER	*createInstance(SERVICE *service, char **options);
static	void	*newSession(ROUTER *instance, SESSION *session);
static	void 	closeSession(ROUTER *instance, void *session);
static	void 	freeSession(ROUTER *instance, void *session);
static	int	routeQuery(ROUTER *instance, void *session, GWBUF *queue);
static	void	diagnostic(ROUTER *instance, DCB *dcb);
static  uint8_t getCapabilities (ROUTER* inst, void* router_session);


static ROUTER_OBJECT MyObject = {
    createInstance,
    newSession,
    closeSession,
    freeSession,
    routeQuery,
    diagnostic,
    NULL,
    NULL,
    getCapabilities
};

/**
 * Implementation of the mandatory version entry point
 *
 * @return version string of the module
 */
char *
version()
{
	return version_str;
}

/**
 * The module initialisation routine, called when the module
 * is first loaded.
 */
void
ModuleInit()
{
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
ROUTER_OBJECT *
GetModuleObject()
{
	return &MyObject;
}

/**
 * Create an instance of the router for a particular service
 * within the gateway.
 * 
 * @param service	The service this router is being create for
 * @param options	The options for this query router
 *
 * @return The instance data for this new instance
 */
static	ROUTER	*
createInstance(SERVICE *service, char **options)
{
    /* Once off processing at the beginning */
    
    WEB_INSTANCE	*inst;

    if ((inst = (WEB_INSTANCE *)malloc(sizeof(WEB_INSTANCE))) == NULL)
        return NULL;

    inst->service = service;
    return (ROUTER *)inst;
}

/**
 * Associate a new session with this instance of the router.
 *
 * @param instance	The router instance data
 * @param session	The session itself
 * @return Session specific data for this session
 */
static	void	*
newSession(ROUTER *instance, SESSION *session)
{
    /* Every time a user connects to the service */
    WEB_SESSION	*wsession;

    if ((wsession = (WEB_SESSION *)malloc(sizeof(WEB_SESSION))) == NULL)
        return NULL;

    wsession->session = session;
    
    int argc = 2;
    char *argv[] = {"maxapi.php", "hello"};
	
    php_embed_init(argc, argv PTSRMLS_CC);
    zend_startup_module(&php_mymod_module_entry);

    /* session->state = SESSION_STATE_READY; */
    /*
    dcb_printf(session->client, "Welcome to the SkySQL MaxScale API Interface (%s).\n",
        version_str);
    * */
    return wsession;
}

/**
 * Close a session with the router, this is the mechanism
 * by which a router may cleanup data structure etc.
 *
 * @param instance	The router instance data
 * @param session	The session being closed
 */
static	void 	
closeSession(ROUTER *instance, void *session)
{
	/* When a user session comes to an end */
}

static void freeSession(
        ROUTER* router_instance,
        void*   router_client_session)
{
	/* Called after all components of the session have been closed */
        php_embed_shutdown(TSRMLS_C);
        
        free(router_client_session);
        return;
}

static	int	
routeQuery(ROUTER *instance, void *session, GWBUF *queue)
{
        WEB_SESSION	*wsession = (WEB_SESSION *)session;
        long int        wsessionint = (uintptr_t) wsession;
        DCB	*dcb = wsession->session->client;

        zend_first_try {
                zval *embedbuffer;
                ALLOC_INIT_ZVAL(embedbuffer);
                char *buffer = (char *)GWBUF_DATA(queue);
                ZVAL_STRING(embedbuffer, buffer, 1);
                ZEND_SET_SYMBOL(&EG(symbol_table), "embedbuffer", embedbuffer);
                
                zval *phpsession;
                char *sessionstring;
                spprintf(&sessionstring, 0, "%ld", (uintptr_t) wsession);
                ALLOC_INIT_ZVAL(phpsession);
                ZVAL_STRING(phpsession, sessionstring, 1);
                ZEND_SET_SYMBOL(&EG(symbol_table), "phpsession", phpsession);
                
                zval *phpdcb;
                char *dcbstring;
                spprintf(&dcbstring, 0, "%ld", (uintptr_t) dcb);
                ALLOC_INIT_ZVAL(phpdcb);
                ZVAL_STRING(phpdcb, dcbstring, 1);
                ZEND_SET_SYMBOL(&EG(symbol_table), "phpdcb", phpdcb);
                
		char *include_script;
		spprintf(&include_script, 0, "include '%s';", "/home/mbrampton/MaxScaleHome/api/apiMaxScale.php");
		zend_eval_string(include_script, NULL, "/home/mbrampton/MaxScaleHome/api/apiMaxScale.php" TSRMLS_CC);
		efree(include_script);
                efree(sessionstring);
                efree(dcbstring);
        } zend_catch {
            int exit_status = EG(exit_status);
            return exit_status;
        } zend_end_try();

        gwbuf_free(queue);
	return 0;
}

/**
 * Diagnostics routine
 *
 * @param	instance	The router instance
 * @param	dcb		The DCB for diagnostic output
 */
static	void
diagnostic(ROUTER *instance, DCB *dcb)
{
	/* Called when user asks to show service */
}

static uint8_t getCapabilities(
        ROUTER*  inst,
        void*    router_session)
{
	/* Not relevant for API */
        return 0;
}
