#include <stdio.h>

#include "account_mon.h"
#include "dcb.h"
#include "hashtable.h"
#include "monitor.h"

extern int __real_check_db_name_after_auth(DCB *, char *, int);

int __wrap_check_db_name_after_auth(DCB *dcb, char *database, int auth_ret) {
    if(database != NULL && strlen(database) > 0) {
        uintptr_t account_id = 0;

        if(strncmp("account_", database, 8) == 0) {
            account_id = strtol(database + 8, NULL, 0);
        } else if(strncmp("account", database, 7) == 0) {
            return auth_ret;
        } else {
            goto ret;
        }

        MONITOR *monitor = monitor_find("account monitor");

        if(monitor == NULL)
            goto ret;

        ACCOUNT_MONITOR *handle = (ACCOUNT_MONITOR *) monitor->handle;

        if(handle == NULL)
            goto ret;

        if(account_monitor_find_shard(handle, account_id) > 0) {
            return auth_ret;
        }
    }
ret:
    return __real_check_db_name_after_auth(dcb, database, auth_ret);
}
