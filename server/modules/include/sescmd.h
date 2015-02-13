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

typedef enum
{
  SNUM_ONE,
  SNUM_ALL,
  SNUM_GOOD
} sescmd_rspnum;

typedef enum
{
  SRES_FIRST,
  SRES_FIRST_GOOD,
  SRES_LAST,
  SRES_LAST_GOOD,
  SRES_TIMEOUT
} sescmd_rsp;

typedef struct sescmd_sem_st
{
  sescmd_rsp reply_on;
  sescmd_rspnum reply_num;
} SCMDSEM;
struct sescmd_list_st;

typedef struct mysql_sescmd_st
{
  GWBUF* buffer; /*< query buffer */
  unsigned char packet_type; /*< packet type */
  bool is_replied; /*< is cmd replied to client */
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
} SCMDCURSOR;

typedef struct sescmd_list_st
{
  SCMD *first; /*< First session command*/
  SCMD *last; /*< Latest session command */
  SCMDCURSOR* cursors; /*< List of cursors for this list */
  SCMDSEM semantics; /*<  */
} SCMDLIST;
SCMDLIST* sescmd_allocate(SCMDSEM* semantics);
void sescmd_append (SCMDLIST* list, GWBUF* buf);
void sescmd_attach (SCMDLIST* list, DCB* dcb);
void sescmd_detach (SCMDLIST* list, DCB* dcb);
void sescmd_execute (SCMDLIST* scur);
#endif	/* SESCMD_H */

