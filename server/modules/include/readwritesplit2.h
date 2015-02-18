#ifndef _RWSPLITROUTER_H
#define _RWSPLITROUTER_H
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
 * @file router.h - The read write split router module heder file
 *
 * @verbatim
 * Revision History
 *
 * See GitHub https://github.com/skysql/MaxScale
 *
 * @endverbatim
 */

#include <dcb.h>
#include <hashtable.h>
#include <sescmd.h>
#include <query_classifier.h>
typedef enum bref_state {
        BREF_IN_USE           = 0x01,
        BREF_WAITING_RESULT   = 0x02, /*< for session commands only */
        BREF_QUERY_ACTIVE     = 0x04, /*< for other queries */
        BREF_CLOSED           = 0x08
} bref_state_t;

#define BREF_IS_NOT_USED(s)         ((s)->bref_state & ~BREF_IN_USE)
#define BREF_IS_IN_USE(s)           ((s)->bref_state & BREF_IN_USE)
#define BREF_IS_WAITING_RESULT(s)   ((s)->bref_num_result_wait > 0)
#define BREF_IS_QUERY_ACTIVE(s)     ((s)->bref_state & BREF_QUERY_ACTIVE)
#define BREF_IS_CLOSED(s)           ((s)->bref_state & BREF_CLOSED)

typedef enum backend_type_t {
        BE_UNDEFINED=-1, 
        BE_MASTER, 
        BE_JOINED = BE_MASTER,
        BE_SLAVE,
        BE_COUNT
} backend_type_t;

struct router_instance;

typedef enum {
	TARGET_UNDEFINED    = 0x00,
        TARGET_MASTER       = 0x01,
        TARGET_SLAVE        = 0x02,
        TARGET_NAMED_SERVER = 0x04,
        TARGET_ALL          = 0x08,
        TARGET_RLAG_MAX     = 0x10
} route_target_t;

#define TARGET_IS_MASTER(t)       (t & TARGET_MASTER)
#define TARGET_IS_SLAVE(t)        (t & TARGET_SLAVE)
#define TARGET_IS_NAMED_SERVER(t) (t & TARGET_NAMED_SERVER)
#define TARGET_IS_ALL(t)          (t & TARGET_ALL)
#define TARGET_IS_RLAG_MAX(t)     (t & TARGET_RLAG_MAX)

typedef struct rses_property_st rses_property_t;
typedef struct router_client_session ROUTER_CLIENT_SES;

typedef enum rses_property_type_t {
        RSES_PROP_TYPE_UNDEFINED=-1,
        RSES_PROP_TYPE_SESCMD=0,
        RSES_PROP_TYPE_FIRST = RSES_PROP_TYPE_SESCMD,
        RSES_PROP_TYPE_TMPTABLES,
        RSES_PROP_TYPE_LAST=RSES_PROP_TYPE_TMPTABLES,
	RSES_PROP_TYPE_COUNT=RSES_PROP_TYPE_LAST+1
} rses_property_type_t;



/**
 * This criteria is used when backends are chosen for a router session's use.
 * Backend servers are sorted to ascending order according to the criteria
 * and top N are chosen.
 */
typedef enum select_criteria {
        UNDEFINED_CRITERIA=0,
        LEAST_GLOBAL_CONNECTIONS, /*< all connections established by MaxScale */
        LEAST_ROUTER_CONNECTIONS, /*< connections established by this router */
        LEAST_BEHIND_MASTER,
        LEAST_CURRENT_OPERATIONS,
        DEFAULT_CRITERIA=LEAST_CURRENT_OPERATIONS,
        LAST_CRITERIA /*< not used except for an index */
} select_criteria_t;


/** default values for rwsplit configuration parameters */
#define CONFIG_MAX_SLAVE_CONN 1
#define CONFIG_MAX_SLAVE_RLAG -1 /*< not used */
#define CONFIG_SQL_VARIABLES_IN TYPE_ALL

#define GET_SELECT_CRITERIA(s)                                                                  \
        (strncmp(s,"LEAST_GLOBAL_CONNECTIONS", strlen("LEAST_GLOBAL_CONNECTIONS")) == 0 ?       \
        LEAST_GLOBAL_CONNECTIONS : (                                                            \
        strncmp(s,"LEAST_BEHIND_MASTER", strlen("LEAST_BEHIND_MASTER")) == 0 ?                  \
        LEAST_BEHIND_MASTER : (                                                                 \
        strncmp(s,"LEAST_ROUTER_CONNECTIONS", strlen("LEAST_ROUTER_CONNECTIONS")) == 0 ?        \
        LEAST_ROUTER_CONNECTIONS : (                                                            \
        strncmp(s,"LEAST_CURRENT_OPERATIONS", strlen("LEAST_CURRENT_OPERATIONS")) == 0 ?        \
        LEAST_CURRENT_OPERATIONS : UNDEFINED_CRITERIA))))
        
/**
 * Property structure
 */
struct rses_property_st {
#if defined(SS_DEBUG)
        skygw_chk_t          rses_prop_chk_top;
#endif
        ROUTER_CLIENT_SES*   rses_prop_rsession; /*< parent router session */
        int                  rses_prop_refcount;
        rses_property_type_t rses_prop_type;
        union rses_prop_data {
		HASHTABLE*	temp_tables;
        } rses_prop_data;
        rses_property_t*     rses_prop_next; /*< next property of same type */
#if defined(SS_DEBUG)
        skygw_chk_t          rses_prop_chk_tail;
#endif
};

/**
 * Internal structure used to define the set of backend servers we are routing
 * connections to. This provides the storage for routing module specific data
 * that is required for each of the backend servers.
 * 
 * Owned by router_instance, referenced by each routing session.
 */
typedef struct backend_st {
#if defined(SS_DEBUG)
        skygw_chk_t     be_chk_top;
#endif
        SERVER*         backend_server;      /*< The server itself */
        int             backend_conn_count;  /*< Number of connections to
					      *  the server
					      */
        bool            be_valid; 	     /*< Valid when belongs to the
					      *  router's configuration
					      */
	int		weight;		     /*< Desired weighting on the
					      *  load. Expressed in .1%
					      * increments
					      */
#if defined(SS_DEBUG)
        skygw_chk_t     be_chk_tail;
#endif
} BACKEND;


/**
 * Reference to BACKEND.
 * 
 * Owned by router client session.
 */
typedef struct backend_ref_st {
#if defined(SS_DEBUG)
        skygw_chk_t     bref_chk_top;
#endif
        BACKEND*        bref_backend;
        DCB*            bref_dcb;
        bref_state_t    bref_state;
        int             bref_num_result_wait;
	GWBUF*          bref_pending_cmd; /*< For stmt which can't be routed due active sescmd execution */
#if defined(SS_DEBUG)
        skygw_chk_t     bref_chk_tail;
#endif
} backend_ref_t;


typedef struct rwsplit_config_st {
        int               rw_max_slave_conn_percent;
        int               rw_max_slave_conn_count;
        select_criteria_t rw_slave_select_criteria;
        int               rw_max_slave_replication_lag;
	target_t          rw_use_sql_variables_in;	
} rwsplit_config_t;
     
/**
 * The client session structure used within this router.
 */
struct router_client_session {
#if defined(SS_DEBUG)
        skygw_chk_t      rses_chk_top;
#endif
        SPINLOCK         rses_lock;      /*< protects rses_deleted                 */
        int              rses_versno;    /*< even = no active update, else odd. not used 4/14 */
        bool             rses_closed;    /*< true when closeSession is called      */
	/** Properties listed by their type */
	rses_property_t* rses_properties[RSES_PROP_TYPE_COUNT];
        backend_ref_t*   rses_master_ref;
        backend_ref_t*   rses_backend_ref; /*< Pointer to backend reference array */
        rwsplit_config_t rses_config;    /*< copied config info from router instance */
        int              rses_nbackends;
        int              rses_capabilities; /*< input type, for example */
        bool             rses_autocommit_enabled;
        bool             rses_transaction_active;
        SCMDLIST*        rses_sescmd_list; /*< Session commands */
	struct router_instance	 *router;	/*< The router instance */
        struct router_client_session* next;
#if defined(SS_DEBUG)
        skygw_chk_t      rses_chk_tail;
#endif
};

/**
 * The statistics for this router instance
 */
typedef struct {
	int		n_sessions;	/*< Number sessions created        */
	int		n_queries;	/*< Number of queries forwarded    */
	int		n_master;	/*< Number of stmts sent to master */
	int		n_slave;	/*< Number of stmts sent to slave  */
	int		n_all;		/*< Number of stmts sent to all    */
} ROUTER_STATS;


/**
 * The per instance data for the router.
 */
typedef struct router_instance {
	SERVICE*                service;     /*< Pointer to service                 */
	ROUTER_CLIENT_SES*      connections; /*< List of client connections         */
	SPINLOCK                lock;	     /*< Lock for the instance data         */
	BACKEND**               servers;     /*< Backend servers                    */
	BACKEND*                master;      /*< NULL or pointer                    */
	rwsplit_config_t        rwsplit_config; /*< expanded config info from SERVICE */
	int                     rwsplit_version;/*< version number for router's config */
        unsigned int	        bitmask;     /*< Bitmask to apply to server->status */
	unsigned int	        bitvalue;    /*< Required value of server->status   */
	ROUTER_STATS            stats;       /*< Statistics for this router         */
        struct router_instance* next;        /*< Next router on the list            */
	bool			available_slaves;
					    /*< The router has some slaves avialable */
        SEMANTICS               semantics; /*< Session command semantics */
} ROUTER_INSTANCE;

#define BACKEND_TYPE(b) (SERVER_IS_MASTER((b)->backend_server) ? BE_MASTER :    \
        (SERVER_IS_SLAVE((b)->backend_server) ? BE_SLAVE :  BE_UNDEFINED));

    
#endif /*< _RWSPLITROUTER_H */
