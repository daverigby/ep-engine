/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * This header file contains the class definition of one of the implementation
 * of the abstract class SequenceList
 */

#pragma once

#include "atomic.h"
#include "config.h"
#include "monotonic.h"
#include "seqlist.h"
#include "stored-value.h"

#include <relaxed_atomic.h>
#include <boost/intrusive/list.hpp>

/* This option will configure "list" to use the member hook */
using MemberHookOption =
        boost::intrusive::member_hook<OrderedStoredValue,
                                      boost::intrusive::list_member_hook<>,
                                      &OrderedStoredValue::seqno_hook>;

/* This list will use the member hook */
using OrderedLL = boost::intrusive::list<OrderedStoredValue, MemberHookOption>;

/**
 * Class that represents a range of sequence numbers.
 * SeqRange is closed, that is, both begin and end are inclusive.
 *
 * Note: begin <= 0 is considered an default/inactive range and can be set
 *       only by ctor or by reset.
 */
class SeqRange {
public:
    SeqRange(const seqno_t beginVal, const seqno_t endVal)
        : end(endVal), begin(beginVal) {
        if ((end < begin) || (begin < 0)) {
            throw std::invalid_argument("Trying to create invalid SeqRange: [" +
                                        std::to_string(begin) + ", " +
                                        std::to_string(end) + "]");
        }
    }

    SeqRange& operator=(const SeqRange& other) {
        begin = other.begin;
        end = other.end;
        return *this;
    }

    /**
     * Returns true if the range overlaps with another.
     */
    bool overlaps(const SeqRange& other) const {
        return std::max(begin, other.begin) <= std::min(end, other.end);
    }

    /**
     *  Returns true if the seqno falls in the range
     */
    bool fallsInRange(const seqno_t seqno) const {
        return (seqno >= begin) && (seqno <= end);
    }

    void reset() {
        begin = 0;
        end = 0;
    }

    void setBegin(const seqno_t start) {
        if ((start <= 0) || (start > end)) {
            throw std::invalid_argument(
                    "Trying to set incorrect begin " + std::to_string(start) +
                    " on SeqRange: [" + std::to_string(begin) + ", " +
                    std::to_string(end) + "]");
        }
        begin = start;
    }

private:
    seqno_t end;
    seqno_t begin;
};

/**
 * This class implements SequenceList as a basic doubly linked list.
 * Uses boost intrusive list for doubly linked list implementation.
 *
 * Intrusive hook is to be added to OrderedStoredValue for it to be used in the
 * BasicLinkedList. Once in the BasicLinkedList, OrderedStoredValue is now
 * shared between HashTable and BasicLinkedList.
 *
 * BasicLinkedList sees only the hook for next and prev; HashTable
 * see only the hook for hashtable chaining.
 *
 * But there should be an agreement on the deletion (invalidation of next and
 * prev link; chaining link) of the elements between these 2 class objects.
 * Currently,
 * (i) HashTable owns a OrderedStoredValue (as a unique_ptr) that is not stale.
 * (ii) It relinquishes the ownership by marking it stale. This happens when
 *      deduplication is not possible and we want to keep an old value around.
 * (iii) BasicLinkedList deletes the stale OrderedStoredValues.
 * (iv) During a Hashtable clear (full or partial), which happens during
 *      VBucket delete or rollback, we first remove the element from
 *      BasicLinkedList (invalidate next, prev links) and then delete from the
 *      hashtable.
 */
class BasicLinkedList : public SequenceList {
public:
    BasicLinkedList(uint16_t vbucketId, EPStats& st);

    ~BasicLinkedList();

    void appendToList(std::lock_guard<std::mutex>& seqLock,
                      OrderedStoredValue& v) override;

    SequenceList::UpdateStatus updateListElem(
            std::lock_guard<std::mutex>& seqLock,
            OrderedStoredValue& v) override;

    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> rangeRead(
            seqno_t start, seqno_t end) override;

    void updateHighSeqno(const OrderedStoredValue& v) override;

    void markItemStale(StoredValue::UniquePtr ownedSv) override;

    uint64_t getNumStaleItems() const override;

    uint64_t getNumDeletedItems() const override;

    uint64_t getNumItems() const override;

    void dump() const override;

protected:
    /* Underlying data structure that holds the items in an Ordered Sequence */
    OrderedLL seqList;

    /**
     * Lock that serializes writes (append, update, cleanStaleItems) on
     * 'seqList'
     */
    mutable std::mutex writeLock;

    /**
     * Used to mark of the range where point-in-time snapshot is happening.
     * To get a valid point-in-time snapshot and for correct list iteration we
     * must not de-duplicate an item in the list in this range.
     */
    SeqRange readRange;

    /**
     * Lock that protects readRange.
     * We use spinlock here since the lock is held only for very small time
     * periods.
     */
    SpinLock rangeLock;

    /* Overall memory consumed by (stale) OrderedStoredValues owned by the
       list */
    Couchbase::RelaxedAtomic<size_t> staleSize;

    /* Metadata memory consumed by (stale) OrderedStoredValues owned by the
       list */
    Couchbase::RelaxedAtomic<size_t> staleMetaDataSize;

private:
    /**
     * Lock that serializes range reads on the 'seqList'.
     * We need to serialize range reads because, range reads set a list level
     * range in which items are read. If we have multiple range reads then we
     * must handle the races in the updation of the range to have most inclusive
     * range.
     * For now we use this lock to allow only one range read at a time.
     */
    std::mutex rangeReadLock;

    /**
     * We need to keep track of the highest seqno separately because there is a
     * small window wherein the last element of the list (though in correct
     * order) does not have a seqno.
     *
     * highseqno is monotonically increasing and is reset to a lower value
     * only in case of a rollback.
     */
    Monotonic<seqno_t> highSeqno;

    /**
     * We need to this to send out point-in-time snapshots in range read
     *
     * highestDedupedSeqno is monotonically increasing and is reset to a lower
     * value only in case of a rollback.
     */
    Monotonic<seqno_t> highestDedupedSeqno;

    /**
     * Indicates the number of elements in the list that are stale (old,
     * duplicate values). Stale items are owned by the list and hence must
     * periodically clean them up.
     */
    uint64_t numStaleItems;

    /**
     * Indicates the number of logically deleted items in the list.
     * Since we are append-only, distributed cache supporting incremental
     * replication, we need to keep deleted items for while and periodically
     * purge them
     */
    uint64_t numDeletedItems;

    /* Used only to log debug messages */
    const uint16_t vbid;

    /* Ep engine stats handle to track stats */
    EPStats& st;

    friend std::ostream& operator<<(std::ostream& os,
                                    const BasicLinkedList& ll);
};

/// Outputs a textual description of the BasicLinkedList
std::ostream& operator <<(std::ostream& os, const BasicLinkedList& ll);
