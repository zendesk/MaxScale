/*
 * This file is distributed as part of the MariaDB Corporation MaxScale.  It is free
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
 * Copyright MariaDB Corporation Ab 2013-2014
 */

/**
 * @file httpd.c - HTTP daemon protocol module
 *
 * The httpd protocol module is intended as a mechanism to allow connections
 * into the gateway for the purpose of accessing information within
 * the gateway with a REST interface
 * databases.
 *
 * In the first instance it is intended to allow a debug connection to access
 * internal data structures, however it may also be used to manage the 
 * configuration of the gateway via REST interface.
 *
 * @verbatim
 * Revision History
 * Date		Who			Description
 * 08/07/2013	Massimiliano Pinto	Initial version
 * 09/07/2013 	Massimiliano Pinto	Added /show?dcb|session for all dcbs|sessions
 *
 * @endverbatim
 */

#include <httpd.h>
#include <gw.h>
#include <modinfo.h>
#include <log_manager.h>
#include <resultset.h>

MODULE_INFO info = {
	MODULE_API_PROTOCOL,
	MODULE_IN_DEVELOPMENT,
	GWPROTOCOL_VERSION,
	"An experimental HTTPD implementation for use in admnistration"
};

/** Defined in log_manager.cc */
extern int            lm_enabled_logfiles_bitmask;
extern size_t         log_ses_count[];
extern __thread log_info_t tls_log_info;

#define HTTP_SERVER_STRING "MaxScale(c) v.1.0.0"

static char *version_str = "V1.0.1";

static int httpd_read_event(DCB* dcb);
static int httpd_write_event(DCB *dcb);
static int httpd_write(DCB *dcb, GWBUF *queue);
static int httpd_error(DCB *dcb);
static int httpd_hangup(DCB *dcb);
static int httpd_accept(DCB *dcb);
static int httpd_close(DCB *dcb);
static int httpd_listen(DCB *dcb, char *config);

static int on_url(http_parser *, const char *, size_t);
static int on_header_field(http_parser *, const char *, size_t);
static int on_header_value(http_parser *, const char *, size_t);
static int on_body(http_parser *, const char *, size_t);
static int on_message_complete(http_parser *);

static http_parser_settings http_settings = {
        .on_url = on_url,
        .on_header_field = on_header_field,
        .on_header_value = on_header_value,
        .on_body = on_body,
        .on_message_complete = on_message_complete
};

/**
 * The "module object" for the httpd protocol module.
 */
static GWPROTOCOL MyObject = { 
	httpd_read_event,			/**< Read - EPOLLIN handler	 */
	httpd_write,				/**< Write - data from gateway	 */
	httpd_write_event,			/**< WriteReady - EPOLLOUT handler */
	httpd_error,				/**< Error - EPOLLERR handler	 */
	httpd_hangup,				/**< HangUp - EPOLLHUP handler	 */
	httpd_accept,				/**< Accept			 */
	NULL,					/**< Connect			 */
	httpd_close,				/**< Close			 */
	httpd_listen,				/**< Create a listener		 */
	NULL,					/**< Authentication		 */
	NULL					/**< Session			 */
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
GWPROTOCOL *
GetModuleObject()
{
	return &MyObject;
}

/**
 * Read event for EPOLLIN on the httpd protocol module.
 *
 * @param dcb	The descriptor control block
 * @return
 */
static int
httpd_read_event(DCB* dcb)
{
        SESSION		*session = dcb->session;
        ROUTER_OBJECT	*router = session->service->router;
        ROUTER		*router_instance = session->service->router_instance;
        void		*rsession = session->router_session;

        http_parser *parser = ((HTTPD_session *) dcb->data)->parser;

        char buf[HTTPD_SMALL_BUFFER];
        ssize_t recved = recv(dcb->fd, buf, HTTPD_SMALL_BUFFER, 0);

        if (recved < 0) {
                  /* Handle error. */
                dcb_close(dcb);
        }

        /* Start up / continue the parser.
         *  * Note we pass recved==0 to signal that EOF has been received.
         *   */
        size_t nparsed = http_parser_execute(parser, &http_settings, buf, recved);

        if (parser->upgrade || nparsed != recved) {
                // Error or upgrade request, just close it
                dcb_close(dcb);
        }

	return 0;
}

/**
 * EPOLLOUT handler for the HTTPD protocol module.
 *
 * @param dcb	The descriptor control block
 * @return
 */
static int
httpd_write_event(DCB *dcb)
{
	return dcb_drain_writeq(dcb);
}

/**
 * Write routine for the HTTPD protocol module.
 *
 * Writes the content of the buffer queue to the socket
 * observing the non-blocking principles of the gateway.
 *
 * @param dcb	Descriptor Control Block for the socket
 * @param queue	Linked list of buffes to write
 */
static int
httpd_write(DCB *dcb, GWBUF *queue)
{
        int rc;
        rc = dcb_write(dcb, queue);
	return rc;
}

/**
 * Handler for the EPOLLERR event.
 *
 * @param dcb	The descriptor control block
 */
static int
httpd_error(DCB *dcb)
{
	dcb_close(dcb);
	return 0;
}

/**
 * Handler for the EPOLLHUP event.
 *
 * @param dcb	The descriptor control block
 */
static int
httpd_hangup(DCB *dcb)
{
	dcb_close(dcb);
	return 0;
}

/**
 * Handler for the EPOLLIN event when the DCB refers to the listening
 * socket for the protocol.
 *
 * @param dcb	The descriptor control block
 */
static int
httpd_accept(DCB *dcb)
{
int	n_connect = 0;

	while (1)
	{
		int			so = -1;
		struct sockaddr_in	addr;
		socklen_t		addrlen;
		DCB			*client = NULL;

		if ((so = accept(dcb->fd, (struct sockaddr *)&addr, &addrlen)) == -1)
			return n_connect;
		else
		{
			atomic_add(&dcb->stats.n_accepts, 1);
			
			if((client = dcb_alloc(DCB_ROLE_REQUEST_HANDLER))){
				client->fd = so;
				client->remote = strdup(inet_ntoa(addr.sin_addr));
				memcpy(&client->func, &MyObject, sizeof(GWPROTOCOL));

                                HTTPD_session *session = calloc(1, sizeof(HTTPD_session));

                                if(session == NULL) { // todo
                                }

                                session->parser = malloc(sizeof(http_parser));

                                if(session->parser == NULL) { // todo
                                }

                                http_parser_init(session->parser, HTTP_REQUEST);

                                // DCB holds the HTTPD_session
                                client->data = session;

                                // Parser holds the DCB
                                session->parser->data = client;
			
				client->session = session_alloc(dcb->session->service, client);

				if (poll_add_dcb(client) == -1)
					{
						close(so);
						return n_connect;
					}
				n_connect++;
			}
		}
	}
	
	return n_connect;
}

/**
 * The close handler for the descriptor. Called by the gateway to
 * explicitly close a connection.
 *
 * @param dcb	The descriptor control block
 */

static int
httpd_close(DCB *dcb)
{
	return 0;
}

/**
 * HTTTP daemon listener entry point
 *
 * @param	listener	The Listener DCB
 * @param	config		Configuration (ip:port)
 */
static int
httpd_listen(DCB *listener, char *config)
{
struct sockaddr_in	addr;
int			one = 1;
int         rc;
int			syseno = 0;

	memcpy(&listener->func, &MyObject, sizeof(GWPROTOCOL));
	if (!parse_bindconfig(config, 6442, &addr))
		return 0;

	if ((listener->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		return 0;
	}

        /* socket options */
	syseno = setsockopt(listener->fd,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   (char *)&one,
                   sizeof(one));

	if(syseno != 0){
		skygw_log_write_flush(LOGFILE_ERROR,"Error: Failed to set socket options. Error %d: %s",errno,strerror(errno));
		return 0;
	}
        /* set NONBLOCKING mode */
        setnonblocking(listener->fd);

        /* bind address and port */
        if (bind(listener->fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
	{
        	return 0;
	}

        rc = listen(listener->fd, SOMAXCONN);
        
        if (rc == 0) {
            LOGIF(LM, (skygw_log_write_flush(LOGFILE_MESSAGE,"Listening httpd connections at %s", config)));
        } else {
            int eno = errno;
            errno = 0;
            fprintf(stderr,
                    "\n* Failed to start listening http due error %d, %s\n\n",
                    eno,
                    strerror(eno));
            return 0;
        }

        
        if (poll_add_dcb(listener) == -1)
	{
		return 0;
	}

	return 1;
}

static int on_message_complete(http_parser *parser) {
        DCB *dcb = parser->data;
        HTTPD_session *session = dcb->data;

        session->url_fields = malloc(sizeof(struct http_parser_url));

        // if(session->url_fields == NULL)
        http_parser_parse_url(session->url, session->url_len, 1, session->url_fields);

        // TODO
        SESSION_ROUTE_QUERY(dcb->session, NULL);

	char date[64] = "";
	const char *fmt = "%a, %d %b %Y %H:%M:%S GMT";
	time_t httpd_current_time = time(NULL);

	strftime(date, sizeof(date), fmt, localtime(&httpd_current_time));
        dcb_printf(dcb, "HTTP/1.1 200 OK\r\nDate: %s\r\nServer: %s\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n", date, HTTP_SERVER_STRING);
        dcb_close(dcb);

        return 0;
}

static int on_url(http_parser *parser, const char *at, size_t length) {
        DCB *dcb = parser->data;
        HTTPD_session *session = dcb->data;

        if(session->url_len == 0) {
                session->url_len = length;
                session->url = malloc(length + 1);

                if(session->url == NULL) { // TODO
                }

                strncpy(session->url, at, length);
        } else {
                session->url_len += length;
                session->url = realloc(session->url, session->url_len + 1);

                if(session->url == NULL) { // TODO
                }

                strncat(session->url, at, length);
        }

        session->url[session->url_len + 1] = '\0';

        return 0;
}

static int on_header_field(http_parser *parser, const char *at, size_t length) {
        return 0;
}

static int on_header_value(http_parser *parser, const char *at, size_t length) {
        return 0;
}

static int on_body(http_parser *parser, const char *at, size_t length) {
        return 0;
}
