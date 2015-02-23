#ifndef SESCMD_H
#define	SESCMD_H
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
 * Copyright MariaDB Corporation Ab 2013-2015
 */

/**
 * @file sescmd.h Session command handling
 * 
 * This file contains structures and functions that handle commands with multiple 
 * recipients. It allows dynamic addition and removal of DCBs and the complete
 * command history is automatically played back when a new DCB is added to the set.
 *
 * @verbatim
 * Revision History
 *
 * Date		Who			Description
 * 17/02/15	Markus Makela		Initial implementation
 *
 * @endverbatim
 */

#include <buffer.h>
#include <log_manager.h>
#include <dcb.h>
#include <server.h>
#include <mysql_client_server_protocol.h>
#include <openssl/sha.h>

#define PEARSON_DIGEST_LEN 16

/** 
 * Minimum number of backend servers that must respond. If less than this value
 * of backend servers respond, it is considered a failure and the session should
 * be closed.
 */
typedef enum
{
  SNUM_ONE,
  SNUM_ALL
} sescmd_rspnum;

/** 
 * When to send the reply to the client. If this is set to SRES_FIRST the first
 * response received will be sent to the client. If it is set to SRES_LAST then 
 * all backend servers that are in the session command list must reply. SRES_MIN
 * will check if a minimum number of servers have responded to a session command
 * and if this is true it will reply to the client.
 */
typedef enum
{
  SRES_FIRST,
  SRES_FIRST_GOOD, /*< To be implemented */
  SRES_LAST,
  SRES_LAST_GOOD, /*< To be implemented */
  SRES_TIMEOUT, /*< To be implemented */
  SRES_MIN,
  SRES_DCB /*< When a specific DCB replies */
} sescmd_rsp;

/** 
 * What to do when a backend responds with an error. If this is set to SERR_DROP
 * the session will continue and the failed servers should be removed from the 
 * session command list. Instead if it is SERR_FAIL_CONN then a single failure 
 * will signal that the session has failed. If the session is considered as failed,
 * the session should be closed.
 */
typedef enum
{
  SERR_DROP, /*< To be implemented */
  SERR_FAIL_CONN /*< To be implemented */
} sescmd_rsperr;

typedef struct semantics_t
  { 
     sescmd_rspnum must_reply; /*< How many must reply */
     sescmd_rsp reply_on; /*< when to send the reply to the client */
     sescmd_rsperr on_error; /*< What to do when an error occurs */
     int min_nreplies; /*< Minimum number of replies that must be received 
                        * before the reply is sent to the client*/
     int timeout; /*<  Backends replying later than this are considere as failed.
                   * Using a non-positive value disables timeouts */
     DCB* master_dcb; /*< The "Master" DCB which is the source with the right
                       * responses, NULL for disabled  */
  }SEMANTICS;

/** What to do when the maximum length list of the has been exceeded */
typedef enum
{
  DROP_LAST,
  DROP_FIRST
} mlen_err_t;

/** List properties */
typedef union list_prop_st{
    int max_len; /*< The maximum length of the list, */
    mlen_err_t on_mlen_err; /*< The action taken when maximum length is exceeded */
}list_prop_t;

struct sescmd_list_st;

typedef struct mysql_sescmd_st
{
  GWBUF* buffer; /*< Stored query */
  unsigned char packet_type; /*< packet type */
  bool reply_sent; /*< If the command been sent to the client */
  int n_replied; /*< Number of replies received */
  SPINLOCK lock;
  struct mysql_sescmd_st* next; /*< The session command that was executed
                                 * after this one */ 
  unsigned char reply_type; /*< Replied packet type */

} SCMD;

typedef struct sescmd_cursor_st
{
  struct sescmd_list_st* scmd_list; /*< Pointer to owning list */
  SCMD* scmd_cur_cmd; /*< Pointer to current session command */
  DCB* backend_dcb; /*< The backend DCB this cursor is associated with */
  bool replied_to; /* Has the backend DCB received a response */
  bool scmd_cur_active; /*< True if command is being executed */
  struct sescmd_cursor_st *next; /*< Next cursor */
  SPINLOCK lock; /*< Cursor spinlock */
} SCMDCURSOR;

typedef struct sescmd_list_st
{
  SCMD *first; /*< First session command */
  SCMD *last; /*< Latest session command */
  SCMDCURSOR* cursors; /*< List of cursors for this list */
  int n_cursors; /*< Number of session command cursors */
  SEMANTICS semantics; /*< The way the session command list behaves */
  list_prop_t properties; /*< Properties of the list */
  SPINLOCK lock;
} SCMDLIST;

SCMDLIST* sescmd_allocate();
void sescmd_free(SCMDLIST*);
bool sescmd_add_command (SCMDLIST* list, GWBUF* buf);
bool sescmd_add_dcb (SCMDLIST* list, DCB* dcb);
bool sescmd_remove_dcb (SCMDLIST* list, DCB* dcb);
bool sescmd_execute_in_backend(DCB* backend_dcb,GWBUF* buffer);
bool sescmd_is_active(SCMDLIST* list, DCB* dcb);
bool sescmd_has_next(SCMDLIST* list, DCB* dcb);
GWBUF* sescmd_get_next(SCMDLIST* list, DCB* dcb);
bool sescmd_process_replies(SCMDLIST* list, DCB* dcb, GWBUF** response);
bool sescmd_handle_failure(SCMDLIST* list, DCB* dcb);
#endif	/* SESCMD_H */

