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

#include <buffer.h>
#include <log_manager.h>
#include <dcb.h>
#include <server.h>
#include <mysql_client_server_protocol.h>

/** 
 * Minimum number of backend servers that must respond
 */
typedef enum
{
  SNUM_ONE,
  SNUM_ALL
} sescmd_rspnum;

/** 
 * When to send the reply to the client
 */
typedef enum
{
  SRES_FIRST,
  SRES_FIRST_GOOD,
  SRES_LAST,
  SRES_LAST_GOOD,
  SRES_TIMEOUT
} sescmd_rsp;

/** 
 * What to do when a backend responds with an error
 */
typedef enum
{
  SERR_DROP,
  SERR_FAIL_CONN
} sescmd_rsperr;


struct sescmd_list_st;

typedef struct mysql_sescmd_st
{
  GWBUF* buffer; /*< query buffer */
  unsigned char packet_type; /*< packet type */
  bool is_replied; /*< is cmd replied to client */
  int n_replied; /*< number of replies received */
  SPINLOCK lock;
  struct mysql_sescmd_st* next;
} SCMD;

typedef struct sescmd_cursor_st
{
  struct sescmd_list_st* scmd_list; /*< pointer to owner property */
  SCMD* scmd_cur_cmd; /*< pointer to current session command */
  DCB* backend_dcb;
  bool replied_to;
  bool scmd_cur_active; /*< true if command is being executed */
  struct sescmd_cursor_st *next; /*< Next cursor */
  SPINLOCK lock;
} SCMDCURSOR;

typedef struct sescmd_list_st
{
  SCMD *first; /*< First session command*/
  SCMD *last; /*< Latest session command */
  SCMDCURSOR* cursors; /*< List of cursors for this list */
  int n_cursors; /*< Number of session command cursors */
  
  union SEMANTICS
  { 
     sescmd_rspnum n_replies; /*< How many must reply */
     sescmd_rsp reply_on; /*< when to send the reply to the client */
     sescmd_rsperr on_error; /*< What to do when an error occurse */
     int timeout; /*< 
                   * Backends replying later than this are considere as failed.
                   * Using a non-positive value disables timeouts. */
  }semantics;
  
  SPINLOCK lock;
} SCMDLIST;

SCMDLIST* sescmd_allocate();
bool sescmd_add_command (SCMDLIST* list, GWBUF* buf);
bool sescmd_add_dcb (SCMDLIST* list, DCB* dcb);
bool sescmd_remove_dcb (SCMDLIST* list, DCB* dcb);
void sescmd_detach (SCMDLIST* list, DCB* dcb);
void sescmd_execute (SCMDLIST* list);
GWBUF* sescmd_process_replies(SCMDLIST* list, DCB* dcb, GWBUF* response);
#endif	/* SESCMD_H */

