/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"
#include "vbucket.h"
#include "seqlist.h"

#include <queue>

/**
 * This class derives from VBucket class and handles the in-memory lifetime of
 * an item in a partition (vbucket) of an 'Ephemeral Bucket'.
 *
 * Operations include sequence number generation, CAS generation,
 * in-memory sequential storage, putting it on a hash table for quick access
 * and on checkpoint for quick in-memory replication of latest items.
 */
class EphemeralVBucket : public VBucket {
public:
    EphemeralVBucket(id_type i,
                     vbucket_state_t newState,
                     EPStats &st,
                     CheckpointConfig &chkConfig,
                     KVShard *kvshard,
                     int64_t lastSeqno,
                     uint64_t lastSnapStart,
                     uint64_t lastSnapEnd,
                     FailoverTable *table,
                     std::shared_ptr<Callback<id_type> > cb,
                     Configuration& config,
                     vbucket_state_t initState = vbucket_state_dead,
                     uint64_t chkId = 1,
                     uint64_t purgeSeqno = 0,
                     uint64_t maxCas = 0);

    virtual ~EphemeralVBucket();

    /**
     * Retrives items sequentially from in-memory sequential storage
     */
    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> inMemoryBackfill(
                                                                uint64_t start,
                                                                uint64_t end);

    /**
     * Cleans all stale items in in-memory sequential storage starting from
     * 'start'
     *
     * @return Last seqno checked by the function. It is upto the caller to
     *         resume next cleanup from that point or from beggining.
     */
    uint64_t cleanStaleItems(uint64_t start);

    uint64_t getNumStaleItems() const {
        return seqList->getNumStaleItems();
    }

protected:

    /* Data structure for in-memory sequential storage */
    std::unique_ptr<SequenceList> seqList;

private:

    /**
     * Puts StoredValue on sequential storage. Sequential storage may choose to
     * create a new StoredValue for this update and retain the current
     * StoredValue as a duplicate.
     *
     * Generates seqno/cas if requried.
     * Updates StoredValue on HT and puts it on chkpt
     *
     * Assumes the hash bucket lock htLock is grabbed
     *
     * return HashTable status
     */
    mutation_type_t updateStoredValue(std::unique_lock<std::mutex>& htLock,
                                      VBSetCtx& vbsetCtx, KVBucket* kvb);

    /**
     * Creates new StoredValue from itm.
     * Generates seqno and cas if required
     * Puts StoredValue on HT, sequential storage and on chkpt
     *
     * Assumes the hash bucket lock htLock is grabbed
     *
     * return HashTable status
     */
    mutation_type_t addNewStoredValue(std::unique_lock<std::mutex>& htLock,
                                      VBSetCtx& vbsetCtx, KVBucket* kvb);

    class GenSeqnoFunc {
    public:
        GenSeqnoFunc(std::unique_lock<std::mutex>& seqLck,
                     EphemeralVBucket& evb,
                     VBSetCtx& vbsetCtx)
            : seqLck(seqLck),
              evb(evb),
              vbsetCtx(vbsetCtx) { }

        void operator() (StoredValue& v) {
            int64_t highSeqno = evb.getHighSeqnoUnlocked(seqLck);;
            Item& itm = vbsetCtx.itm;
            GenerateBySeqno& genBySeqno = vbsetCtx.generateBySeqno;
            GenerateCas& genCas = vbsetCtx.generateCas;

            if(GenerateBySeqno::Yes == genBySeqno) {
                ++highSeqno;
                /* [EPHE TODO]: Once we generate seqno in one single place, that is here
                 we do not need the enum
                 GenerateBySeqno::AlreadyGenerated */
                genBySeqno = GenerateBySeqno::AlreadyGenerated;
            } else {
                if (itm.getBySeqno() <= highSeqno) {
                    throw std::logic_error("EphemeralVBucket::addNewStoredValue(): "
                                           "vb " + std::to_string(evb.getId()) +
                                           ": item seqno (" +
                                           std::to_string(itm.getBySeqno()) +
                                           ") <= highSeqno (" +
                                           std::to_string(highSeqno));
                }
                highSeqno = itm.getBySeqno();
            }
            v.setBySeqno(highSeqno);
            itm.setBySeqno(highSeqno);

            if (TrackCasDrift::Yes == vbsetCtx.trackCasDrift) {
                evb.setMaxCasAndTrackDrift(v.getCas());
            }

            // MB-20798: Allow the HLC to be created 'atomically' with the seqno as
            // we're holding the ::seqLock.
            if (GenerateCas::Yes == vbsetCtx.generateCas) {
                itm.setCas(evb.nextHLCCas());
                v.setCas(itm.getCas());
                genCas = GenerateCas::AlreadyGenerated;
            }
        }
        std::unique_lock<std::mutex>& seqLck;
        EphemeralVBucket& evb;
        VBSetCtx& vbsetCtx;
    };
};
