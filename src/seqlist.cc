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

#include "seqlist.h"

#include <mutex>
#include <algorithm>

BasicLinkedList::BasicLinkedList(HashTable& hashTable,
                                 const uint16_t vbucketId)
    : SequenceList(hashTable, vbucketId),
      readRange(0, 0),
      numItems(0),
      numStaleItems(0),
      highSeqno(0)
{
}

BasicLinkedList::~BasicLinkedList() {
    /* Delete only stale items here, other items are deleted by the hash
       table */
    seqList.remove_and_dispose_if([](const StoredValue& v) {
                                      return v.isStale();
                                  },
                                  [](StoredValue* v) {
                                      delete v;
                                  });
}

void BasicLinkedList::appendToList(std::unique_lock<std::mutex>& htLock,
                                   std::unique_lock<std::mutex>& seqLock,
                                   StoredValue& v) {
    /* Allow only one write to the list at a time */
    std::lock_guard<std::mutex> lckGd(writeLock);

    /* Needed so that the rangeRead gets a correct snaphot till a consistent
       highSeqno */
    std::lock_guard<SpinLock> lh(rangeLock);

    ++numItems;
    appendToList_UNLOCKED(lckGd, v);
}

void BasicLinkedList::appendToList_UNLOCKED(std::lock_guard<std::mutex>& lh,
                                            StoredValue& v) {
    seqno_t seqno = v.getBySeqno();
    if (seqno <= highSeqno) {
        throw std::logic_error("BasicLinkedList::appendToList_UNLOCKED(): (vb "
                               + std::to_string(vbid) + ") seqno (" +
                               std::to_string(seqno) + ") <  highSeqno ("
                               + std::to_string(highSeqno) + ")");
    }

    seqList.push_back(v);
    highSeqno = seqno;
}

std::pair<StoredValue*, mutation_type_t> BasicLinkedList::updateListElem(
                                std::unique_lock<std::mutex>& htLock,
                                std::unique_lock<std::mutex>& seqLock,
                                StoredValue& v, Item& itm, bool hasMetaData,
                                std::function<void(StoredValue&)>& seqGenObj)
{
    /* May have to create a StoredValue copy if there is a rangeRead in the
       range the update is itended */
    StoredValue* newStoredValue = &v;

    /* Allow only one write to the list at a time */
    std::lock_guard<std::mutex> lckGd(writeLock);

    uint64_t currSeqno = static_cast<uint64_t>(v.getBySeqno());

    /* Lock that needed for consistent read of SeqRange 'readRange' */
    std::lock_guard<SpinLock>  lh(rangeLock);
    if (readRange.fallsInRange(currSeqno)) {
        /* Range read is in middle of a snapshot we cannot move the element to
           the end of the list. Hence mark curr StoredValue as duplicate and
           create a new StoredValue and append it to the end of the list */

        /* HT always points to the latest StoredValue, hence the stale
           StoredValue must be unlinked in the hashbucket of the hash table */
        ht.unlocked_remove(htLock, v.getKey());

        /* HT function to create a new StoredValue.
           (The way our HT and StoredValue are implemented, the creation of an
            StoredValue is tied to HT.
            [EPHE TODO]: Decouple StoredValue creation from HT
           ) */
        newStoredValue = ht.unlocked_copyStoredValue(htLock, itm);

        /* Mark curr StoredValue as duplicate (stale) */;
        v.markStale();
        ++numStaleItems;
        ++numItems;
    } else {
        OrderedLL::iterator currItr = seqList.iterator_to(v);
        /* Remove curr StoredValue from this position in the list */
        seqList.erase(currItr);
    }

    /* Update the StoredValue with the item */
    mutation_type_t rv = ht.unlocked_updateStoredValue(htLock, *newStoredValue,
                                                       itm, hasMetaData);

    /* Generate seqno and cas */
    seqGenObj(*newStoredValue);

    /* Append the newly created StoredValue to the end of the list */
    appendToList_UNLOCKED(lckGd, *newStoredValue);

    return std::make_pair(newStoredValue, rv);
}

std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>>
                    BasicLinkedList::rangeRead(seqno_t start, seqno_t end) {
    std::vector<queued_item> items;
    seqno_t last;

    if (start > end) {
        return std::make_pair(ENGINE_ERANGE, items);
    }

    /* Allows only 1 rangeRead for now */
    std::lock_guard<std::mutex> lckGd(rangeReadLock);

    OrderedLL::iterator it;
    /* Lock the range for snapshot */
    {
        std::lock_guard<SpinLock> lh(rangeLock);
        if (start >= highSeqno) {
            /* If the request is for an invalid range, return before iterating
               through the list */
            return std::make_pair(ENGINE_ERANGE, items);
        }
        readRange.set(0, highSeqno); /* [EPHE TODO]: Start from 'start',
                                                     instead of 0 ? */
        /* We will not get consistent point-in-time snapshot beyond this
           seqno as our read range is only until highSeqno at the time
           rangeRead starts */
        last = highSeqno;
        it = seqList.begin();
    }

    for (; it != seqList.end(); ++it) {
        const StoredValue& v = *it;
        {
            std::lock_guard<SpinLock> lh(rangeLock);
            readRange.min = v.getBySeqno(); /* [EPHE TODO]: should we update
                                                         the min every time ? */
        }
        if (v.getBySeqno() > start) {
            queued_item itmCpy = nullptr;
            try {
                itmCpy = v.toItem(false, vbid);
            } catch (const std::bad_alloc&) {
                LOG(EXTENSION_LOG_WARNING, "BasicLinkedList::rangeRead(): "
                    "(vb %d) ENOMEM while trying to copy "
                    "item with seqno %" PRIi64 "before streaming it",
                    vbid, v.getBySeqno());
                /* delete all items */
                items.clear();
                return std::make_pair(ENGINE_ENOMEM, items);
            }
            items.push_back(itmCpy);
        }
        if (v.getBySeqno() == last) {
            break;
        }
    }

    /* Done with range read, reset the range */
    {
        std::lock_guard<SpinLock> lh(rangeLock);
        readRange.reset();
    }
    return std::make_pair(ENGINE_SUCCESS, items);
}

seqno_t BasicLinkedList::cleanStaleItems(seqno_t start) {
    /* cleanStaleItems() and writes() are serialized */
    std::lock_guard<std::mutex> lckGd(writeLock);

    /* Move to start */
    OrderedLL::iterator it = std::find_if(seqList.begin(), seqList.end(),
                                          [start](StoredValue& v) {
                                              return (v.getBySeqno() >= start);
                                          });

    while (it != seqList.end()) {
        StoredValue& v = *it;
        start = v.getBySeqno();

        /* Lock that needed for consistent read of SeqRange 'readRange' */
        std::lock_guard<SpinLock> lh(rangeLock);
        if (readRange.fallsInRange(start)) {
            /* return the point until where stale items are cleaned, it is
               upto the caller to call this function again at another time */
            return start;
        }

        if (v.isStale()) {
            /* Remove stale StoredValue from this position in the list */
            it = seqList.erase(it);
            delete (&v);
            --numStaleItems;
        } else {
            ++it;
        }
    }
    return highSeqno;
}

uint64_t BasicLinkedList::getNumStaleItems() const {
    std::lock_guard<std::mutex> lckGd(writeLock);
    return numStaleItems;
}

void BasicLinkedList::registerFakeRangeRead() {
    std::lock_guard<std::mutex> lckGd(writeLock);
    std::lock_guard<SpinLock> lh(rangeLock);
    readRange.set(1, highSeqno);
}

void BasicLinkedList::unRegisterFakeRangeRead() {
    std::lock_guard<std::mutex> lckGd(writeLock);
    std::lock_guard<SpinLock> lh(rangeLock);
    readRange.reset();
}

std::vector<seqno_t> BasicLinkedList::getAllSeqnosForVerification() const {
    std::vector<seqno_t> allSeqnos;
    std::lock_guard<std::mutex> lckGd(writeLock);

    for (auto& val : seqList) {
        //std::cout << val.getKey().c_str() << ": " <<  val.getBySeqno() << std::endl;
        allSeqnos.push_back(val.getBySeqno());
    }
    return allSeqnos;
}
