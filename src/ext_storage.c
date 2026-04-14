/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "ext_storage.h"
#include "server.h"
#include <stdbool.h>

const int move_command_dbid_arg_index = 2;
const int copy_command_destination_key_index = 2;

static list *keys_pending_ext_storage = NULL; // pending keys to/from external storage

// Configuration parameters
int ext_data_enabled = 0;
int max_num_concurrent_items_spilled = 10;
int items_spillover_batch_size = 10;

// State variables
static int num_items_spilling_to_disk = 0;
static struct evictionPoolEntry *spillPoolLRU = NULL;

static char dummy_value[] = "VXKeHogKgJ=[5V9_X^b?48OKF2jGA<f:iR@50o7dS3JV4Q6L68lC[GTA]0DaMg?_oSmcS2^N1J?ELSX@CfKQ7cM5aea\\ngY8a3LGgNVa9eRA46XS8>7ABe1>Jl9O\\Rm\\7IS:g\\U5b]=`ePo^LXhl4e:4P[DQNA3cgn<RH4UcdI6<V9S78NViC;^omK:<<Ldel:=iDmLB8B6m?K=8269Pj>VGVSSh^J6;;DOVS`3jV`6IY\\e85iH9_G[UELFknTb;Nfj2leC34;li[ECO7>=<Zmco2NM_aB`?]5GSGS25LjQf>]OV0NO1SIgoS1_Z1kg^_L\\S]C9IVF]W9=AcY151C3_mcL36Vh10LAbHeMPFIfHTE=PPi?iSbCC?X8BKFQ5N]mQLXHoD]dJ5Y564XOaogPK>jMEo<=kNIg`6mMN[KV[Sn<jg_:V]J3oCQb2;mEJJIII_]4ecKT2D:kcA6WA6DCgeFPBHTALYTiC]_Tl4T8Ib8Z[[T@XPLZk<AonmfHgSc_FJkFZg^XWD^`HL1KTE[N2`9VlB]4UPmEI=:biSaMSBdVdlFPmlh76jf^5`1ZLij1FND@PPVn75CTeSJi[]lom0A_A]YUF4S;3U?7F65T9321DY6lV1^LQLA0HSM;9K`=Jkf`E^5WSViF:VA1V>6fXU_jBXFe^EHMAfnRC^NN;5ak=ccRS^mW<cRegG;km;CCQ`Qg9P9RkhR7FWUeEh:53OS9n5ed^CI930G5`Km;YG3DMJOCc4VY6oY=Q;;ii7TE]nGTngca>k<gJTGCgZ8Z>]\\JG25RFnY@H3I^\\=R[Rkf;5N8>G3YQA0U7G1L6m@RAlUFibc3BM_hVFNWL=U0V8@;^KP8nVE7aAn6M83=ng_;Q4n_E?Z6A[fGUIg0R9U>8?UOdVDh6XCVC9EE@GYbib804;Nl9MIb^^bKD4nm3Xc1FLkCVKklgEPoUW^e<L7XjVnbFJID=_XE0d8Q^DfMS<E;NVmR3l9";

// Defined in evict.c
int evictionPoolPopulate(serverDb *db, kvstore *samplekvs, struct evictionPoolEntry *pool);
sds findBestEvictionCandidate(struct evictionPoolEntry *pool, int *bestdbid, int *bestslot);

void extStorage_init(void) {
    if (!ext_data_enabled) return;

    serverLog(LL_DEBUG, "extStorage_init(): Initializing the external storage...");
    spillPoolLRU = zcalloc(sizeof(struct evictionPoolEntry) * EVPOOL_SIZE);
    for (int j = 0; j < EVPOOL_SIZE; j++) {
        spillPoolLRU[j].cached = sdsnewlen(NULL, EVPOOL_CACHED_SDS_SIZE);
    }
    keys_pending_ext_storage = listCreate();
}

/* Determines if the key blocks the client command or not.
 * Returns 1 if the key blocks client, 0 otherwise
 */
static int keyBlocksClient(serverDb *db, sds key, bool is_write_cmd) {
    // write gets blocked on a key that is moving to or from external storage
    if (is_write_cmd && hashtableFind(db->keys_to_ext_storage, key, NULL)) {
        serverLog(LL_DEBUG, "Key %s is in transit to external storage, blocks write client", key);
        return 1;
    }
    int ret = dbFind(db, key) == NULL;
    serverLog(LL_DEBUG, "keyBlocksClient() determined the key %s %s the client", key, ret ? "blocks" : "doesn't");
    return ret;
}

extStorageKeyValueMsg *createStorageMessage(int type, int db_id, robj* key, robj* value, long long expireMs) {
    serverAssert(key);
    extStorageKeyValueMsg *msg = (extStorageKeyValueMsg*)zmalloc(sizeof(extStorageKeyValueMsg));
    msg->msg_type = type;
    msg->db_id = db_id;
    msg->key = key;
    incrRefCount(key);
    msg->value = value;
    //if (value) incrRefCount(value);
    msg->ttl = expireMs;
    return msg;
}

int preCommandExec(client *c) {
    if (!ext_data_enabled) return CMD_FILTER_ACCEPT;
    serverDb *current_db = c->db;
    // Get the indices of the keys for the command
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
            // All keys with missing values are collected to correlate
            // keys and blocked clients.
            blocking_keys[num_keys_to_block++] = c->argv[keys[i].pos];
            // TODO: for each of the keys, if it is not in the keys_to_ext_storage set, submit a request to fetch it
            if (hashtableAdd(current_db->keys_to_ext_storage, sdsdup(key_str))) {
                // Notify the storage layer to fetch the value of the item
                serverLog(LL_DEBUG, "Requesting key %s in DB %d from external storage...", key_str, current_db->id);
                extStorageKeyValueMsg *msg = createStorageMessage(EXT_STORAGE_MSG_TYPE_READ, current_db->id, c->argv[keys[i].pos], NULL, 1000000 + server.mstime);
                listAddNodeTail(keys_pending_ext_storage, msg);
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
    // If any of the keys are not present, return REJECT.
    return num_keys_to_block > 0 ? CMD_FILTER_REJECT : CMD_FILTER_ACCEPT;
}

static void processCompletedStorageRequests(void) {
    listIter li;
    listNode *ln;
    listRewind(keys_pending_ext_storage, &li);

    while((ln = listNext(&li))) {
        extStorageKeyValueMsg *msg = listNodeValue(ln);
        int db_id = msg->db_id;
        robj *key = (robj*)msg->key;
        //serverLog(LL_NOTICE, "Processing %d type storage request on key: %s in db: %d", (int)msg->msg_type, (char*)objectGetVal(key), db_id);
        // The key must exist in the keys_to_ext_storage set. Otherwise it is a logic bug.
        // TODO: unless we allow storage layer to delete items on its own like active expiration/eviction
        serverAssert(hashtableDelete(server.db[db_id]->keys_to_ext_storage, (sds)objectGetVal(key)));
        switch (msg->msg_type) {
            case EXT_STORAGE_MSG_TYPE_READ: {
                robj *new_value = createStringObject(dummy_value, strlen(dummy_value));
                dbAdd(server.db[db_id], key, &new_value);
                if (msg->ttl > 0) {
                    setExpire(NULL, server.db[db_id], key, msg->ttl * 1000);
                }
                break;
            }
            case EXT_STORAGE_MSG_TYPE_WRITE: {
                // Delete the item synchronously
                if (dbGenericDelete(server.db[db_id], key, 0, DB_FLAG_KEY_NONE) == 0) {
                    serverLog(LL_DEBUG, "dbGenericDelete() returns 0, failed to delete item...");
                }
                break;
            }
            default:
                break;
        }
        unblockClientsInUseOnKey(key);
        listDelNode(keys_pending_ext_storage, ln);
    }
}

// A helper function that determines if a dbEntry has an embedded value. 
// It does this by checking the robject encoding.
static bool isEmbeddedObject(dbEntry *o) {
    return (o->encoding == OBJ_ENCODING_EMBSTR || o->encoding == OBJ_ENCODING_INT);
}

// Returns -1 if the key can't be spilled, return 0 if the key is spilled.
static int spillItemAsync(sds key, int db_id) {
    serverAssert(key != NULL && db_id >= 0 && server.db[db_id]);
    dbEntry *item = dbFind(server.db[db_id], key);
    //sds key = objectGetKey(item);
    // We cannot spill an item that is embedded object.
    if (isEmbeddedObject(item)) {
        serverLog(LL_DEBUG, "Key is embedded. Can't spill...");
        return -1;
    }
    // We cannot spill an item whose ref count is greater than 1
    if (item->refcount != 1) {
        serverLog(LL_DEBUG, "Item %s refcount is %d, can't spill...", key, (int)item->refcount);
        return -1;
    }

    // TODO: prevent rehashing while the entry before spilling the item to external storage.

    // TODO: module API implementation.
    // Notify the storage layer to spill the item
    if (hashtableAdd(server.db[db_id]->keys_to_ext_storage, sdsdup(key))) {
        robj *keyobj = createStringObject(key, sdslen(key));
        long long expireMs = objectGetExpire(item);
        serverLog(LL_DEBUG, "Moving key %s in DB %d to external storage with TTL %lld", key, db_id, expireMs);
        extStorageKeyValueMsg *msg = createStorageMessage(EXT_STORAGE_MSG_TYPE_WRITE, db_id, keyobj, item, expireMs);
        listAddNodeTail(keys_pending_ext_storage, msg);
    }
    return 0;
}

int processCompletedStorageRequestsAndSpillOldItems(void) {
    if (!ext_data_enabled) return 0;
    if (server.maxmemory == 0) return 0; // Unlimited maxmemory

    processCompletedStorageRequests();

    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION) {
        serverLog(LL_DEBUG, "noeviction policy is not supported for spilling items...");
        return 0;
    }

    if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_ERR) {
        // Spill a batch of oldest item using async storage IO
        num_items_spilling_to_disk = 0;
        for (int i = 0; i < items_spillover_batch_size; i++) {
            // If the number of items in flight to disk is beyond the limit, exit
            if (num_items_spilling_to_disk >= max_num_concurrent_items_spilled) {
                return 0;
            }
            // Find a batch of oldest items
            int best_dbid;
            int best_slot;
            sds best_key = findBestEvictionCandidate(spillPoolLRU, &best_dbid, &best_slot);
            if (best_key == NULL) {
                serverLog(LL_DEBUG, "Did not find the best key for spilling...");
                /* No evictable key found across all DBs. */
                return 0;
            }
            if (spillItemAsync(best_key, best_dbid) == -1) {
                // If the ASIO layer is unable to take the request to spill item to disk,
                // clean up the asio_entry object and return. We will retry next time.
                serverLog(LL_DEBUG, "Unable to spill key %s to disk...", best_key);
                return 0;
            } else {
                // We successfully spilled the item
                num_items_spilling_to_disk++;
                serverLog(LL_DEBUG, "successfully spilled key from DB: %d, slot: %d...", best_dbid, best_slot);
            }
        }
    }
    return 0;
}


