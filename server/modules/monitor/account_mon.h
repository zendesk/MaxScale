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
#include	<librdkafka/rdkafka.h>

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

        // accounts storage
        HASHTABLE *accounts;

        // kafka config
        rd_kafka_t *connection;
} ACCOUNT_MONITOR;

#define MONITOR_RUNNING		1
#define MONITOR_STOPPING	2
#define MONITOR_STOPPED		3

#define MONITOR_INTERVAL 10000 // in milliseconds
#define MONITOR_DEFAULT_ID 1UL // unsigned long value
