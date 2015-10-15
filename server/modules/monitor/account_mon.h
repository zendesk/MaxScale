#include <server.h>
#include <spinlock.h>
#include <hashtable.h>

#include <librdkafka/rdkafka.h>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>

typedef struct {
        SPINLOCK lock;
        pthread_t tid;

        int shutdown;
        int status;

        unsigned long id;

        // accounts storage
        HASHTABLE *accounts;

        // kafka config
        rd_kafka_t *connection;
        rd_kafka_conf_t *configuration;
        rd_kafka_topic_t *topic;
        rd_kafka_queue_t *queue;
        const struct rd_kafka_metadata *metadata;

        char *topic_name;

        bool connected;

        zhandle_t *zookeeper;

        char **brokerlist;
} ACCOUNT_MONITOR;

#define MONITOR_RUNNING		1
#define MONITOR_STOPPING	2
#define MONITOR_STOPPED		3

#define MONITOR_INTERVAL 10000 // in milliseconds
#define MONITOR_DEFAULT_ID 1UL // unsigned long value

uintptr_t account_monitor_find_shard(ACCOUNT_MONITOR *, uintptr_t);
