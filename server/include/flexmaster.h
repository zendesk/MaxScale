#ifndef __FLEXMASTER_H
#define __FLEXMASTER_H

typedef struct {
        int transactions_open;
        SPINLOCK transaction_lock;
        DCB **waiting_clients;
} FLEXMASTER_FILTER_INSTANCE;

#endif
