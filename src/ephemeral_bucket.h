/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "kv_bucket.h"

/**
 * Ephemeral Bucket
 *
 * A bucket type without any persistent data storage. Similar to memcache (default)
 * buckets, except with VBucket goodness - replication, rebalance, failover.
 */

class EphemeralBucket : public KVBucket {
public:
    EphemeralBucket(EventuallyPersistentEngine& theEngine);

    /// File stats not supported for Ephemeral buckets.
    ENGINE_ERROR_CODE getFileStats(const void *cookie,
                                   ADD_STAT add_stat) override{
        return ENGINE_KEY_ENOENT;
    }

    /// Disk stats not supported for Ephemeral buckets.
    ENGINE_ERROR_CODE getPerVBucketDiskStats(const void* cookie,
                                             ADD_STAT add_stat) override {
        return ENGINE_KEY_ENOENT;
    }

    /**
     * Creates an EphemeralVBucket
     */
    RCPtr<VBucket> createVBucket(VBucket::id_type id,
                                 vbucket_state_t state, KVShard* shard,
                                 int64_t lastSeqno,
                                 uint64_t lastSnapStart, uint64_t lastSnapEnd,
                                 std::unique_ptr<FailoverTable> table,
                                 std::shared_ptr<Callback<VBucket::id_type> > cb,
                                 vbucket_state_t initState, uint64_t chkId,
                                 uint64_t purgeSeqno, uint64_t maxCas) override;
};
