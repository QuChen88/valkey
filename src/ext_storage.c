/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "ext_storage.h"
#include "server.h"
#include "module.h"
#include <stdbool.h>

const int COMPLETED_STORAGE_REQUESTS_PROCESSING_BATCH_SIZE = 10;

// Configuration parameters
int ext_data_enabled = 0;
int max_num_concurrent_items_spilled = 100;
int items_spillover_batch_size = 10;

// Metrics
static long long total_items_spilled_to_ext_storage = 0;
static long long total_items_fetched_from_ext_storage = 0;
static long long total_items_spilling_to_ext_storage = 0;
static long long total_items_fetching_from_ext_storage = 0;

// State variables
static struct evictionPoolEntry *spillPoolLRU = NULL;
static ValkeyModuleExternalStorageMsg **completed_storage_requests = NULL;

// Defined in evict.c
int evictionPoolPopulate(serverDb *db, kvstore *samplekvs, struct evictionPoolEntry *pool);
sds findBestEvictionCandidate(struct evictionPoolEntry *pool, int *bestdbid, int *bestslot);

void extStorage_init(void) {
    if (!ext_data_enabled) return;

    serverLog(LL_NOTICE, "Initializing the external storage...");
    completed_storage_requests = zcalloc(COMPLETED_STORAGE_REQUESTS_PROCESSING_BATCH_SIZE * sizeof(ValkeyModuleExternalStorageMsg*));
    spillPoolLRU = zcalloc(sizeof(struct evictionPoolEntry) * EVPOOL_SIZE);
    for (int j = 0; j < EVPOOL_SIZE; j++) {
        spillPoolLRU[j].cached = sdsnewlen(NULL, EVPOOL_CACHED_SDS_SIZE);
    }
}

/**
 * Determines if the key blocks the client command or not.
 * Returns 1 if the key blocks client, 0 otherwise
 */
static int keyBlocksClient(serverDb *db, sds key, bool is_write_cmd) {
    // If the key is confirmed to not be in external storage, allow the command.
    // Note: The current implementation removes the key from keys_not_in_ext_storage set upon the
    // next command that needs to access the key. Ideally we want to only do that when the key
    // is created via a write command and not for a read command for better efficiency. However
    // that requires more complex book-keeping algorithm where if the key was never written to,
    // we don't keep it in the keys_not_in_ext_storage set forever which consumes memory.
    if (hashtableDelete(db->keys_not_in_ext_storage, key)) {
        serverLog(LL_DEBUG, "Key %s is not in external storage, allow the client command", key);
        return 0;
    }

    // If the key is moving to or from external storage, block the write command on the key
    if (is_write_cmd) {
        if (hashtableFind(db->keys_to_ext_storage, key, NULL)) {
            serverLog(LL_DEBUG, "Key %s is in transit to external storage, blocks write client", key);
            return 1;
        }
    }
    return dbFind(db, key) == NULL;
}

ValkeyModuleExternalStorageMsg *createStorageMessage(int type, int db_id, robj* key, robj* value, long long expireMs) {
    serverAssert(key);
    ValkeyModuleExternalStorageMsg *msg = (ValkeyModuleExternalStorageMsg*)zmalloc(sizeof(ValkeyModuleExternalStorageMsg));
    msg->msg_type = type;
    msg->status = 0; // Core doesn't need to indicate message status. The module does.
    msg->ttl = expireMs;
    msg->db_id = db_id;
    msg->key = key;
    incrRefCount(key);
    msg->value = value;
    return msg;
}

int preCommandExec(client *c) {
    if (!ext_data_enabled) return CMD_FILTER_ACCEPT;

    // For data tiering, we first try to process the completed storage
    // requests and unblock previous clients. This needs to be done prior
    // to starting any processing for the current client because we don't
    // want to further delay the previously blocked clients as they have
    // strictly higher priority over newly incoming clients.
    // In addition, we want to spill old items to disk if the memory usage
    // is above the spill to disk memory threshold.
    processCompletedStorageRequestsAndSpillOldItems();

    // Determine if the client command should be blocked by the keys it uses
    serverDb *current_db = c->db;
    getKeysResult result;
    initGetKeysResult(&result);
    int num_keys = getKeysFromCommand(c->cmd, c->argv, c->argc, &result);
    if (num_keys == 0)  return 0;

    keyReference *keys = result.keys;
    bool is_write_cmd = (c->cmd)->flags & CMD_WRITE;
    int num_keys_to_block = 0;
    robj **blocking_keys = (robj **)zmalloc(sizeof(robj *) * num_keys);
    for (int i = 0; i < num_keys; i++) {
        sds key_str = objectGetVal(c->argv[keys[i].pos]);
        bool key_will_block = false;
        // TODO: handle MOVE and COPY commands
        key_will_block = keyBlocksClient(current_db, key_str, is_write_cmd) || key_will_block;
        if (key_will_block) {
            // All keys with missing values are collected which block the client command.
            blocking_keys[num_keys_to_block++] = c->argv[keys[i].pos];
            // For each of the keys, if it is not in the keys_to_ext_storage set, submit a request to fetch it
            if (hashtableAdd(current_db->keys_to_ext_storage, sdsdup(key_str))) {
                // Notify the storage layer to fetch the value of the item
                serverLog(LL_DEBUG, "Requesting key %s in DB %d from external storage...", key_str, current_db->id);
                ValkeyModuleExternalStorageMsg *msg = createStorageMessage(VALKEYMODULE_EXTERNAL_STORAGE_MSG_TYPE_READ,
                                                          current_db->id, c->argv[keys[i].pos], NULL, 0);
                moduleFireExternalStorageEvent(msg);
                total_items_fetching_from_ext_storage++;
            } else {
                serverLog(LL_DEBUG, "Unable to add key to keys_to_ext_storage hashtable: %s", key_str);
            }
        }
    }

    getKeysFreeResult(&result);
    if (num_keys_to_block > 0) {
        c->flag.pending_command = 1;
        blockClientInUseOnKeys(c, num_keys_to_block, blocking_keys);
    }
    zfree(blocking_keys);
    // If the client gets blocked by some keys, return REJECT the command
    return num_keys_to_block > 0 ? CMD_FILTER_REJECT : CMD_FILTER_ACCEPT;
}

static void processCompletedStorageRequests(void) {
    int next_batch_size = moduleGetCompletedExternalStorageResponses(completed_storage_requests, COMPLETED_STORAGE_REQUESTS_PROCESSING_BATCH_SIZE);
    if (next_batch_size == 0) return;

    for (int j = 0; j < next_batch_size; j++) {
        ValkeyModuleExternalStorageMsg *msg = completed_storage_requests[j];
        int db_id = msg->db_id;
        robj *key = (robj*)msg->key;
        // The key must exist in the keys_to_ext_storage set. Otherwise it is a logic bug.
        // TODO: unless we allow storage layer to delete items on its own like active expiration/eviction
        sds key_name = (sds)objectGetVal(key);
        serverLog(LL_DEBUG, "Processing completed storage request %d for key %s in DB %d", msg->msg_type, key_name, db_id);
        serverAssert(hashtableDelete(server.db[db_id]->keys_to_ext_storage, key_name));
        switch (msg->msg_type) {
            case VALKEYMODULE_EXTERNAL_STORAGE_MSG_TYPE_READ: {
                robj *new_value = (robj*) msg->value;
                if (new_value != NULL) {
                    // Key is found in the storage layer. We add it into the DB
                    dbAdd(server.db[db_id], key, &new_value);
                    if (msg->ttl > 0) {
                        setExpire(NULL, server.db[db_id], key, msg->ttl);
                    }
                    total_items_fetched_from_ext_storage++;
                } else {
                    // If the value is NULL for a READ storage response, then the key is not found in the
                    // storage layer. We track the key as absent in the DB
                    hashtableAdd(server.db[db_id]->keys_not_in_ext_storage, sdsdup(key_name));
                }
                total_items_fetching_from_ext_storage--;
                break;
            }
            case VALKEYMODULE_EXTERNAL_STORAGE_MSG_TYPE_WRITE: {
                // When write request completes in storage layer, delete the item synchronously
                if (msg->status != VALKEYMODULE_OK) {
                    serverLog(LL_WARNING, "Failed to write the key %s to external storage. Keeping the item in memory.", key_name);
                } else {
                    if (dbGenericDelete(server.db[db_id], key, 0, DB_FLAG_KEY_NONE) > 0) {
                        total_items_spilled_to_ext_storage++;
                        serverLog(LL_DEBUG, "Successfully deleted from memory key: %s", key_name);
                    }
                }
                total_items_spilling_to_ext_storage--;
                break;
            }
            default:
                break;
        }
        // Unblock clients on this key
        unblockClientsInUseOnKey(key);
        decrRefCount(key); // This key is referenced in ValkeyModuleExternalStorageMsg, free it.
        zfree(msg);
    }
}

// A helper function that determines if a dbEntry has an embedded value. 
// It does this by checking the robject encoding.
static bool isEmbeddedObject(dbEntry *o) {
    return (o->encoding == OBJ_ENCODING_EMBSTR || o->encoding == OBJ_ENCODING_INT);
}

/**
 * Asynchronously spill a key to external storage.
 * Returns 0 if the key is being spilled, returns -1 otherwise.
 */
static int spillItemAsync(sds key, int db_id) {
    serverAssert(key != NULL && db_id >= 0 && server.db[db_id]);
    dbEntry *item = dbFind(server.db[db_id], key);
    // We cannot spill an item that is embedded object.
    if (isEmbeddedObject(item)) {
        serverLog(LL_DEBUG, "Can't spill an embedded key.");
        return -1;
    }
    // We cannot spill an item whose ref count is greater than 1
    if (item->refcount != 1) {
        serverLog(LL_DEBUG, "Can't spill key %s with refcount is %d", key, (int)item->refcount);
        return -1;
    }

    // TODO: prevent rehashing while the entry before spilling the item to external storage.

    // If the key is not already in transit to storage layer, then notify the storage layer to spill it
    if (moduleHasExternalStorageSubscribers() && hashtableAdd(server.db[db_id]->keys_to_ext_storage, sdsdup(key))) {
        robj *keyobj = createStringObject(key, sdslen(key));
        long long expireMs = objectGetExpire(item);
        serverLog(LL_DEBUG, "Moving key %s in DB %d to external storage with TTL %lld", key, db_id, expireMs);
        ValkeyModuleExternalStorageMsg *msg = createStorageMessage(VALKEYMODULE_EXTERNAL_STORAGE_MSG_TYPE_WRITE, db_id, keyobj, item, expireMs);
        moduleFireExternalStorageEvent(msg);
        return 0;
    }
    return -1;
}

/**
 * Process a batch of completed storage requests and spill a batch of old items if we are over maxmemory.
 * Returns the total number of keys being spilled to external storage.
 * Returns 0 if this function did a no-op and no keys are being spilled.
 */
int processCompletedStorageRequestsAndSpillOldItems(void) {
    if (!ext_data_enabled) return 0;
    if (server.maxmemory == 0) return 0; // Unlimited maxmemory

    processCompletedStorageRequests();

    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION) {
        serverLog(LL_DEBUG, "noeviction policy is not supported for spilling items...");
        return 0;
    }

    int num_items_spilling = 0;
    if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_ERR) {
        // Asynchronously spill a batch of oldest item to the storage layer
        while (num_items_spilling < items_spillover_batch_size) {
            // If too many items are in flight to external storage, stop spilling more items
            if (total_items_spilling_to_ext_storage >= max_num_concurrent_items_spilled) break;

            // Find a batch of oldest items
            int best_dbid;
            int best_slot;
            sds best_key = findBestEvictionCandidate(spillPoolLRU, &best_dbid, &best_slot);
            if (best_key == NULL) {
                // No evictable key found across all DBs
                serverLog(LL_DEBUG, "Did not find the best key for spilling...");
                break;
            }

            // If the key is already in transit to external storage, then skip it
            if (hashtableFind(server.db[best_dbid]->keys_to_ext_storage, best_key, NULL)) {
                serverLog(LL_DEBUG, "key %s already in transit", best_key);
                break;
            }

            if (spillItemAsync(best_key, best_dbid) == -1) {
                // If the storage layer is unable to take the request to spill item to disk,
                // clean up the asio_entry object and return. We will retry next time.
                serverLog(LL_DEBUG, "Unable to spill key %s to disk...", best_key);
                break;
            } else {
                // We successfully spilled the item
                num_items_spilling++;
                total_items_spilling_to_ext_storage++;
            }
        }
    }
    return total_items_spilling_to_ext_storage;
}

/**
 * Adds external storage information to info's output.
 *
 * After the call, the passed sds info string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 */
sds genExternalStorageInfoString(sds info) {
    info = sdscatprintf(info,
                     "total_num_items_spilled_to_ext_storage:%lld\r\n"
                     "total_num_items_fetched_from_ext_storage:%lld\r\n"
                     "num_items_spilling_to_ext_storage:%lld\r\n"
                     "num_items_fetching_from_ext_storage:%lld\r\n",
                     total_items_spilled_to_ext_storage,
                     total_items_fetched_from_ext_storage,
                     total_items_spilling_to_ext_storage,
                     total_items_fetching_from_ext_storage);
    return info;
}
