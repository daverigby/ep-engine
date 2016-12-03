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

/**
 * Seqlist.h contains class definitions of the classes that implement an
 * ordered sequence of items in memory. The classes are not generic, they only
 * work on 'StoredValue' which is the in-memory representation of an item
 */

#include "config.h"
#include "stored-value.h"
#include "hash_table.h"

#include <deque>
#include <boost/intrusive/list.hpp>

/* [EPHE TODO]: Check if uint64_t can be used instead */
using seqno_t = int64_t;

/**
 * Class that represents a range of sequence numbers
 */
class SeqRange {
public:
    SeqRange(const seqno_t minVal, const seqno_t maxVal)
        : max(maxVal),
          min(minVal) {}

    void set(const seqno_t minVal, const seqno_t maxVal) {
        min = minVal;
        max = maxVal;
    }

    /* Returns true if the range overlaps with another */
    bool overlap(const SeqRange& other) const {
        if (std::max(min, other.min) <= std::min(max, other.max)) {
            return true;
        }
        return false;
    }

    /* Returns true if the seqno falls in the range */
    bool fallsInRange(const seqno_t seqno) const {
        if ((seqno >= min) && (seqno <= max)) {
            return true;
        }
        return false;
    }

    void reset() {
        min = 0;
        max = 0;
    }

    seqno_t max;
    seqno_t min;
};

/**
 * SequenceList is the abstract base class for the classes that hold ordered
 * sequence of items in memory. To store/retreive items in memory in our
 * (multi-threaded, monotonically increasing seq model) we need these classes
 * around basic data structures like LL or Skiplist.
 *
 * 'StoredValue' is the in-memory representation of an item and SequenceList
 * derivatives store a sequence of 'StoredValues'. Based on the implementation
 * of SequenceList might have to carry an intrusive component that is necessary
 * for the list.
 *
 * A sequence list implementation may have to work in tandem with another
 * data structure that holds in-memory items, 'HashTable'. In one such
 * implementation, a SequenceList object and a HashTable object are contained
 * in the in-memory vbucket (partition), 'EphemeralVBucket'.
 *
 * Note: These classes only handle the position of the 'StoredValue' in the
 *       ordered sequence desired. They do not update a StoredValue except for
 *       marking a StoredValue 'stale' and facilitating the creation of a new
 *       StoredValue when necessary.
 *
 *       In the file 'item', 'StoredValue' are used interchangeably
 *
 * [EPHE TODO]: create a documentation (with diagrams) explaining write, update,
 *              rangeRead and Clean Up
 */
class SequenceList {
public:
    SequenceList(HashTable& hashTable, const uint16_t vbucketId) :
        ht(hashTable),
        vbid(vbucketId) {}

    virtual ~SequenceList() {}

    /* Add a new item at the end of the list */
    virtual void appendToList(std::unique_lock<std::mutex>& htLock,
                              std::unique_lock<std::mutex>& seqLock,
                              StoredValue& v) = 0;

    /**
     * Updates the item already in the list and moves it to the end of the list
     *                          (OR)
     * Marks the item stale and facilitates the creation of a new item at the
     * appropriate position in the HashTable and adds the same at the end of
     * the list.
     *
     * Assumes the hash bucket lock htLock is grabbed
     *
     * @return: Newly created StoredValue or nullptr
     *
     * NOTE: The newly created StoredValue is NOT STALE and is owned by the
     *       HashTable
     */
    virtual std::pair<StoredValue*, mutation_type_t> updateListElem(
                            std::unique_lock<std::mutex>& htLock,
                            std::unique_lock<std::mutex>& seqLock,
                            StoredValue& v, Item& itm, bool hasMetaData,
                            std::function<void(StoredValue&)>& seqGenObj) = 0;

    /**
     * Provides point-in-time snapshots which can be used for incremental
     * replication.
     * Copies the StoredValues starting from 'start + 1' seqno into 'items' as a
     * snapshot.
     * Since we use monotonically increasing point-in-time snapshots we cannot
     * guarentee that the snapshot ends at end. Due to dedups we may have to
     * send till highSeqno in the snapshot.
     *
     * @return: ENGINE_ENOMEM or ENGINE_SUCCESS and items in the snapshot
     */
    virtual std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> rangeRead(
                                                            seqno_t start,
                                                            seqno_t end) = 0;

    /**
     * Iterates through the list and deletes all stale items from start
     * Assumes that the stale values are not part of the hashtable.
     *
     * @return Last seqno checked by the function. It is upto the caller to
     *          resume next cleanup from that point or from beggining
     */
    virtual seqno_t cleanStaleItems(seqno_t start) = 0;

    /**
     * @return Returns number of stale items at a intance of time
     */
    virtual uint64_t getNumStaleItems() const = 0;

protected:
    HashTable& ht;

    const uint16_t vbid;
};

/* This option will configure "list" to use the member hook */
typedef boost::intrusive::member_hook<StoredValue,
                                      boost::intrusive::list_member_hook<>,
                                      &StoredValue::hook_> MemberHookOption;

/* This list will use the member hook */
typedef boost::intrusive::list<StoredValue, MemberHookOption> OrderedLL;

/**
 * This class implements SequenceList as a basic doubly linked list.
 * Uses boost intrusive list for doubly linked list implementation.
 *
 * The implementation is quite simple with writes, that is, append, update,
 * cleanStaleItems, all serialized. And only one range read is allowed at a
 * time.
 *
 * Also rangeReads() and cleanStaleItems() start from the beggining of the list
 *
 * Note: These classes only handle the position of the 'StoredValue' in the
 *       ordered sequence desired. They do not update a StoredValue except for
 *       marking a StoredValue 'stale' and facilitating the creation of a new
 *       StoredValue when necessary.
 *
 *       In the file 'item', 'StoredValue' are used interchangeably
 */
class BasicLinkedList : public SequenceList {
public:
    BasicLinkedList(HashTable& hashTable, const uint16_t vbucketId);

    ~BasicLinkedList();

    void appendToList(std::unique_lock<std::mutex>& htLock,
                      std::unique_lock<std::mutex>& seqLock,
                      StoredValue& v);

    std::pair<StoredValue*, mutation_type_t> updateListElem(
                                std::unique_lock<std::mutex>& htLock,
                                std::unique_lock<std::mutex>& seqLock,
                                StoredValue& v, Item& itm, bool hasMetaData,
                                std::function<void(StoredValue&)>& seqGenObj);

    /**
     * Copies the StoredValues starting from 'start + 1' seqno into 'items' as a
     * snapshot.
     *
     * Only one rangeRead can run at a time. Other requests wait till any
     * pending rangeRead is completed.
     * Even though it copies items from start, it begins from the head of the
     * list.
     */
    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> rangeRead(
                                                                seqno_t start,
                                                                seqno_t end);

    /**
     * Iterates through the list and deletes all stale items from start.
     * Assumes that the stale values are not part of the hashtable.
     *
     * Even though it checks for stale items from start, it begins from the
     * head of the list.
     * Clean stale items stops when it encounters an ongoing rangeRead.
     *
     * @return: Last seqno checked by the function. It is upto the caller to
     *          resume next cleanup from that point or from beggining
     */
    seqno_t cleanStaleItems(seqno_t start);

    uint64_t getNumStaleItems() const;

protected:
    /* ONLY used by module tests */
    void registerFakeRangeRead();

    /* ONLY used by module tests */
    void unRegisterFakeRangeRead();

    /* ONLY used by module tests */
    std::vector<seqno_t> getAllSeqnosForVerification() const;

private:
    void appendToList_UNLOCKED(std::lock_guard<std::mutex>& lh,
                               StoredValue& v);

    /* Underlying data structure that holds the items in an Ordered Sequence */
    OrderedLL seqList;

    /**
     * Lock that serializes writes (append, update, cleanStaleItems) on
     * 'seqList'
     */
    mutable std::mutex writeLock;

    /**
     * Lock that serializes rangeReads on'seqList'.
     * We need to serialize rangeReads because rangeReads set a global range
     * in which items are read. If we have multiple rangeReads then we must
     * handle the races in the updation of global read range.
     */
    std::mutex rangeReadLock;

    /*
     * Lock that ensures reads and write on read range is atomic
     * We use spinlock here since the lock is held only for very small time
     * periods.
     */
    SpinLock rangeLock;

    /* All ranges assume that seqno num 0 is never used as a valid seqno */
    SeqRange readRange;

    /* Total number of items in the list */
    uint64_t numItems;

    uint64_t numStaleItems;

    /*uint64_t numDeletedItems; Not yet used */

    seqno_t highSeqno;
};
