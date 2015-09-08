#ifndef __FLEXMASTER_H
#define __FLEXMASTER_H

typedef struct {
        int transactions_open;
        SPINLOCK transaction_lock;
} FLEXMASTER_FILTER_INSTANCE;

#endif
