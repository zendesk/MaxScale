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

/*
 * Revision History
 *
 * Date		Who			Description
 * 08-07-2013	Massimiliano Pinto	Added HTTPD protocol header file 
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dcb.h>
#include <buffer.h>
#include <service.h>
#include <session.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <router.h>
#include <poll.h>
#include <atomic.h>
#include <gw.h>
#include <http_parser.h>

#define HTTPD_SMALL_BUFFER 1024 * 80
#define HTTPD_MAX_HEADER_LINES 2000

struct httpd_header {
        char *field;
        size_t field_len;
        char *value;
        size_t value_len;
};

/**
 * HTTPD session specific data
 *
 */
typedef struct httpd_session {
        http_parser *parser;
        struct http_parser_url *url_fields;

        int method;

        size_t url_len;
	char *url;

        size_t body_len;
        char *body;

        size_t headers_len;
        struct httpd_header headers[HTTPD_MAX_HEADER_LINES];
} HTTPD_session;

void httpd_respond_error(DCB *, int, char *);
