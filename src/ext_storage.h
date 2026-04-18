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

typedef struct serverObject dbEntry;

extern int ext_data_enabled;
extern int max_num_concurrent_items_spilled;
extern int items_spillover_batch_size;


void extStorage_init(void);

int preCommandExec(client *c);

int processCompletedStorageRequestsAndSpillOldItems(void);

#endif

