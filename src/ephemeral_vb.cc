/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include "ephemeral_vb.h"

EphemeralVBucket::EphemeralVBucket(id_type i,
                                   vbucket_state_t newState,
                                   EPStats &st,
                                   CheckpointConfig &chkConfig,
                                   KVShard *kvshard,
                                   int64_t lastSeqno,
                                   uint64_t lastSnapStart,
                                   uint64_t lastSnapEnd,
                                   std::unique_ptr<FailoverTable> table,
                                   std::shared_ptr<Callback<id_type> > cb,
                                   Configuration& config,
                                   vbucket_state_t initState,
                                   uint64_t chkId,
                                   uint64_t purgeSeqno,
                                   uint64_t maxCas)
    : VBucket(i, newState, st, chkConfig, kvshard, lastSeqno, lastSnapStart,
              lastSnapEnd, std::move(table), cb, config, initState, chkId,
              purgeSeqno, maxCas),
      seqList(new BasicLinkedList(ht, i)) {
}

EphemeralVBucket::~EphemeralVBucket() {
}

std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>>
            EphemeralVBucket::inMemoryBackfill(uint64_t start, uint64_t end) {
    return seqList->rangeRead(start, end);
}

uint64_t EphemeralVBucket::cleanStaleItems(uint64_t start) {
    return seqList->cleanStaleItems(start);
}

mutation_type_t EphemeralVBucket::updateStoredValue(
                                        std::unique_lock<std::mutex>& htLock,
                                        VBSetCtx& vbsetCtx, KVBucket* kvb)
{
    /* At this stage we are still unsure that whether the existing StoredValue
       of the item in the memory will be updated or a new StoredValue will be
       created.
       In-memory SeqList module will decide that the operation of adding a new
       StoredValue or updating the existing one should happen atomically in
       the In-memory SeqList module.
       Hence offload the operations of (i) HT updation and (ii) Seqno generation
       to the SeqList module */

    std::unique_lock<std::mutex> lck(seqLock);

    /* Add to the sequential storage, HT update and genSeqno */
    std::function<void(StoredValue&)> cbkFuncObj = GenSeqnoFunc(lck, *this,
                                                                vbsetCtx);
    std::pair<StoredValue*, mutation_type_t> res = seqList->updateListElem(
                                                        htLock, lck,
                                                        *(vbsetCtx.v),
                                                        vbsetCtx.itm,
                                                        vbsetCtx.hasMetaData,
                                                        cbkFuncObj);
    StoredValue* newSV = res.first;
    mutation_type_t rv = res.second;

    /* If a new StoredValue is created for this update, then that must be
       updated and queued onto the chk pt */
    vbsetCtx.v = newSV;

    /* Put on checkpoint or on tap backfill queue */
    if (vbsetCtx.isTap) {
        tapQueueDirty(lck, kvb, *(vbsetCtx.v), *(vbsetCtx.plh),
                      vbsetCtx.generateBySeqno);
    } else {
        chkQueueDirty(lck, kvb, *(vbsetCtx.v), vbsetCtx.generateBySeqno,
                      vbsetCtx.generateCas);
    }
    return rv;
}

mutation_type_t EphemeralVBucket::addNewStoredValue(
                                           std::unique_lock<std::mutex>& htLock,
                                           VBSetCtx& vbsetCtx,
                                           KVBucket* kvb)
{
    mutation_type_t rv = ht.unlocked_addNewStoredValue(htLock, vbsetCtx.v,
                                                       vbsetCtx.itm,
                                                       vbsetCtx.hasMetaData);

    /* Generate seqno if needed */
    std::unique_lock<std::mutex> lck(seqLock);
    genSeqnoAndCas(lck, vbsetCtx, kvb);

    /* Add to the sequential storage */
    seqList->appendToList(htLock, lck, *(vbsetCtx.v));

    /* Put on checkpoint or on tap backfill queue */
    if (vbsetCtx.isTap) {
        tapQueueDirty(lck, kvb, *(vbsetCtx.v), *(vbsetCtx.plh),
                      vbsetCtx.generateBySeqno);
    } else {
        chkQueueDirty(lck, kvb, *(vbsetCtx.v), vbsetCtx.generateBySeqno,
                      vbsetCtx.generateCas);
    }
    return rv;
}
