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

#include "ephemeral_bucket.h"

#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "ephemeral_vb.h"

EphemeralBucket::EphemeralBucket(EventuallyPersistentEngine& theEngine)
    : EPBucket(theEngine) { }

ENGINE_ERROR_CODE EphemeralBucket::setVBucketState_UNLOCKED(uint16_t vbid,
                                                            vbucket_state_t to,
                                                            bool transfer,
                                                            bool notify_dcp,
                                                            LockHolder& vbset) {
    RCPtr<VBucket> vb = vbMap.getBucket(vbid);
    if (vb && to == vb->getState()) {
        return ENGINE_SUCCESS;
    }

    if (vb) {
        vbucket_state_t oldstate = vb->getState();

        vb->setState(to);

        if (oldstate != to && notify_dcp) {
            bool closeInboundStreams = false;
            if (to == vbucket_state_active && !transfer) {
                /**
                 * Close inbound (passive) streams into the vbucket
                 * only in case of a failover.
                 */
                closeInboundStreams = true;
            }
            engine.getDcpConnMap().vbucketStateChanged(vbid, to,
                                                       closeInboundStreams);
        }

        if (to == vbucket_state_active && oldstate == vbucket_state_replica) {
            /**
             * Update snapshot range when vbucket goes from being a replica
             * to active, to maintain the correct snapshot sequence numbers
             * even in a failover scenario.
             */
            vb->checkpointManager.resetSnapshotRange();
        }

        if (to == vbucket_state_active && !transfer) {
            const snapshot_range_t range = vb->getPersistedSnapshot();
            if (range.end == vbMap.getPersistenceSeqno(vbid)) {
                vb->failovers->createEntry(range.end);
            } else {
                vb->failovers->createEntry(range.start);
            }
        }

        if (oldstate == vbucket_state_pending &&
            to == vbucket_state_active) {
            ExTask notifyTask = new PendingOpsNotification(engine, vb);
            ExecutorPool::get()->schedule(notifyTask, NONIO_TASK_IDX);
        }
        scheduleVBStatePersist(vbid);
    } else if (vbid < vbMap.getSize()) {
        FailoverTable* ft = new FailoverTable(engine.getMaxFailoverEntries());
        KVShard* shard = vbMap.getShardByVbId(vbid);
        std::shared_ptr<Callback<uint16_t> > cb(new NotifyFlusherCB(shard));
        Configuration& config = engine.getConfiguration();
        RCPtr<VBucket> newvb(new EphemeralVBucket(vbid, to, stats,
                                                  engine.getCheckpointConfig(),
                                                  shard, 0, 0, 0, ft, cb,
                                                  config));

        if (config.isBfilterEnabled()) {
            // Initialize bloom filters upon vbucket creation during
            // bucket creation and rebalance
            newvb->createFilter(config.getBfilterKeyCount(),
                                config.getBfilterFpProb());
        }

        // The first checkpoint for active vbucket should start with id 2.
        uint64_t start_chk_id = (to == vbucket_state_active) ? 2 : 0;
        newvb->checkpointManager.setOpenCheckpointId(start_chk_id);
        if (vbMap.addBucket(newvb) == ENGINE_ERANGE) {
            return ENGINE_ERANGE;
        }
        vbMap.setPersistenceCheckpointId(vbid, 0);
        vbMap.setPersistenceSeqno(vbid, 0);
        vbMap.setBucketCreation(vbid, true);
        scheduleVBStatePersist(vbid);
    } else {
        return ENGINE_ERANGE;
    }
    return ENGINE_SUCCESS;
}
