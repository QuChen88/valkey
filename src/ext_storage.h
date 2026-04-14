/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef EXT_STORAGE_H
#define EXT_STORAGE_H

#include "server.h"

/* Module command filter status code */
#define CMD_FILTER_ACCEPT 0
#define CMD_FILTER_REJECT 1

#define EXT_STORAGE_MSG_TYPE_READ 0
#define EXT_STORAGE_MSG_TYPE_WRITE 1

typedef struct serverObject dbEntry;

extern int ext_data_enabled;
extern int max_num_concurrent_items_spilled;
extern int items_spillover_batch_size;

typedef struct {
    // Holds the type of the message.
    int msg_type;

    // Holds the database Id for the corresponding data.
    int db_id;

    // This is the key.
    void *key;

    // This is the value.
    void *value;

    long long ttl; // TTL in milliseconds
} extStorageKeyValueMsg;

void extStorage_init(void);

int preCommandExec(client *c);

int processCompletedStorageRequestsAndSpillOldItems(void);

#endif

