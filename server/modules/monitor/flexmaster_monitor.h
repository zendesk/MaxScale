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
#include	<server.h>
#include	<service.h>
#include	<spinlock.h>
#include	<mysql.h>

/**
 * The handle for an instance of a MySQL Monitor module
 */
typedef struct {
	SPINLOCK  lock;			/**< The monitor spinlock */
	pthread_t tid;			/**< id of monitor thread */ 
	int    	  shutdown;		/**< Flag to shutdown the monitor thread */
	int       status;		/**< Monitor status */
	unsigned long   interval;	/**< Monitor sampling interval */
	unsigned long         id;	/**< Monitor ID */
	int	connect_timeout;	/**< Connect timeout in seconds for mysql_real_connect */
	int	read_timeout;		/**< Timeout in seconds to read from the server.
					 * There are retries and the total effective timeout value is three times the option value.
					 */
	int	write_timeout;		/**< Timeout in seconds for each attempt to write to the server.
					 * There are retries and the total effective timeout value is two times the option value.
					 */

        // flexmaster filter
        SERVICE *service;
        char *account_database;

        SERVER *current_server;
        MYSQL *connection;
} MYSQL_MONITOR;

#define MONITOR_RUNNING		1
#define MONITOR_STOPPING	2
#define MONITOR_STOPPED		3

#define MONITOR_INTERVAL 10000 // in milliseconds
#define MONITOR_DEFAULT_ID 1UL // unsigned long value
