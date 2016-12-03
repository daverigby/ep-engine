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

#include "config.h"

#include <gtest/gtest.h>
#include <platform/cb_malloc.h>

#include "bgfetcher.h"
#include "item.h"
#include "ephemeral_vb.h"

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) { }
};

class MockBasicLinkedList : public BasicLinkedList {
public:
    MockBasicLinkedList(HashTable& hashTable, const uint16_t vbucketId)
        : BasicLinkedList(hashTable, vbucketId) { }

    void public_registerFakeRangeRead() {
        registerFakeRangeRead();
    }

    void public_unRegisterFakeRangeRead() {
        unRegisterFakeRangeRead();
    }

    std::vector<seqno_t> public_getAllSeqnosForVerification() {
        return getAllSeqnosForVerification();
    }
};

class MockEphemeralVBucket : public EphemeralVBucket {
public:
    MockEphemeralVBucket(id_type i,
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
                         uint64_t maxCas = 0) :
        EphemeralVBucket(0, newState,
                         st, chkConfig,
                         /*kvshard*/nullptr,
                         /*lastSeqno*/0,
                         /*lastSnapStart*/0,
                         /*lastSnapEnd*/0,
                         /*table*/nullptr,
                         cb,
                         config) {
            /* seqList will rather point to a MockBasicLinkedList in tests */
            seqList.reset(new MockBasicLinkedList(ht, i));
        }

    ~MockEphemeralVBucket() { }

    void public_registerFakeRangeRead() {
        MockBasicLinkedList& mockLL = dynamic_cast<MockBasicLinkedList&>(
                                                                *seqList.get());
        mockLL.public_registerFakeRangeRead();
    }

    void public_unRegisterFakeRangeRead() {
        MockBasicLinkedList& mockLL = dynamic_cast<MockBasicLinkedList&>(
                                                                *seqList.get());
        mockLL.public_unRegisterFakeRangeRead();
    }

    std::vector<seqno_t> public_getAllSeqnosForVerification() {
        MockBasicLinkedList& mockLL = dynamic_cast<MockBasicLinkedList&>(
                                                                *seqList.get());
        return mockLL.public_getAllSeqnosForVerification();
    }
};

class EphemeralVBTest : public ::testing::Test {
protected:
    void SetUp() {
        vbucket.reset(new MockEphemeralVBucket(0,
                                               vbucket_state_active,
                                               global_stats,
                                               checkpoint_config,
                                                /*kvshard*/nullptr,
                                               /*lastSeqno*/0,
                                               /*lastSnapStart*/0,
                                               /*lastSnapEnd*/0,
                                               /*table*/nullptr,
                                               std::make_shared<DummyCB>(),
                                               config));
    }

    void TearDown() {
        vbucket.reset();
    }

    std::unique_ptr<MockEphemeralVBucket> vbucket;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
};

TEST_F(EphemeralVBTest, SetItems) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");
    std::vector<seqno_t> expSeqno;
    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
        expSeqno.push_back(i + 1);
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getHighSeqno());
    auto actualSeqno = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno, actualSeqno);
}

TEST_F(EphemeralVBTest, Update_first) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");
    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    /* Update the first element in the list */
    {
        std::string key("key" + std::to_string(0));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_DIRTY,
                  this->vbucket->set(item, evictionPolicy));
    }
    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems + 1 /* +1 for the update */,
              this->vbucket->getHighSeqno());

    std::vector<seqno_t> expSeqno = {2, 3, 4};
    auto actualSeqno = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno, actualSeqno);
}

TEST_F(EphemeralVBTest, Update_mid) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");
    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    /* Update a middle element in the list */
    {
        std::string key("key" + std::to_string(numItems - 2));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_DIRTY,
                  this->vbucket->set(item, evictionPolicy));
    }
    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems + 1 /* +1 for the update */,
              this->vbucket->getHighSeqno());

    std::vector<seqno_t> expSeqno = {1, 3, 4};
    auto actualSeqno = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno, actualSeqno);
}

TEST_F(EphemeralVBTest, Update_last) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");
    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    /* Update the last element in the list */
    {
        std::string key("key" + std::to_string(numItems - 1));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_DIRTY,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems + 1 /* +1 for the update */,
              this->vbucket->getHighSeqno());

    std::vector<seqno_t> expSeqno = {1, 2, 4};
    auto actualSeqno = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno, actualSeqno);
}

TEST_F(EphemeralVBTest, Update) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");
    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    /* Update an element in the list */
    {
        std::string key("key" + std::to_string(1));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
                  /*data*/data.c_str(),
                  /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_DIRTY,
                  this->vbucket->set(item, evictionPolicy));
    }

    /* Add a new element after updates */
    {
        std::string key("key" + std::to_string(numItems));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems + 1, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems + 2 /* +1 for the update; +1 for add after update */,
              this->vbucket->getHighSeqno());

    std::vector<seqno_t> expSeqno = {1, 3, 4, 5};
    auto actualSeqno = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno, actualSeqno);
}

TEST_F(EphemeralVBTest, TestRangeRead) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");

    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getHighSeqno());

    /* Now do a range read */
    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> res =
                                this->vbucket->inMemoryBackfill(0, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, res.first);
    std::vector<queued_item>& items = res.second;
    EXPECT_EQ(numItems, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());

    /* Free all items */
    items.clear();
}

TEST_F(EphemeralVBTest, TestSetAndCleanUpDuringRangeRead) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");

    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getHighSeqno());

    /* Register a fake RangeRead */
    this->vbucket->public_registerFakeRangeRead();

    /* Update the 3 items when the range read is happening */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_DIRTY,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getNumStaleItems());

    /* Since we have an ongoing rangeRead we must have the update duplicates
       (i.e stale items) */
    std::vector<seqno_t> expSeqno1 = {1, 2, 3, 4, 5, 6};
    auto actualSeqno1 = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno1, actualSeqno1);

    /* Unregister a fake RangeRead */
    this->vbucket->public_unRegisterFakeRangeRead();

    /* Run a clean up job */
    this->vbucket->cleanStaleItems(0);
    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(0, this->vbucket->getNumStaleItems());

    /* Expect all stale items gone */
    std::vector<seqno_t> expSeqno2 = {4, 5, 6};
    auto actualSeqno2 = this->vbucket->public_getAllSeqnosForVerification();
    EXPECT_EQ(expSeqno2, actualSeqno2);
}

TEST_F(EphemeralVBTest, TestRangeReadNonZeroStart) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3, start = 1;
    const std::string data("data");

    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getHighSeqno());

    /* Now do a range read */
    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> res =
                            this->vbucket->inMemoryBackfill(start, numItems);

    EXPECT_EQ(ENGINE_SUCCESS, res.first);
    std::vector<queued_item>& items = res.second;
    EXPECT_EQ(numItems - start, items.size());
    EXPECT_EQ(numItems, items.back()->getBySeqno());

    /* Free all items */
    items.clear();
}

TEST_F(EphemeralVBTest, TestRangeReadGreaterThanHighSeqnoStart) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3, start = 3;
    const std::string data("data");

    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getHighSeqno());

    /* Now do a range read */
    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> res =
                               this->vbucket->inMemoryBackfill(start, numItems);

    EXPECT_EQ(ENGINE_ERANGE, res.first);
    std::vector<queued_item>& items = res.second;
    EXPECT_EQ(numItems - start, items.size());

    /* Free all items */
    items.clear();
}

TEST_F(EphemeralVBTest, TestRangeReadStartGreaterThanEnd) {
    const auto evictionPolicy = item_eviction_policy_t::VALUE_ONLY;
    EXPECT_EQ(0, this->vbucket->getNumItems(evictionPolicy));

    const int numItems = 3;
    const std::string data("data");

    /* Add 3 new items */
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
            /*data*/data.c_str(),
            /*ndata*/static_cast<uint16_t>(data.length())};
        EXPECT_EQ(WAS_CLEAN,
                  this->vbucket->set(item, evictionPolicy));
    }

    EXPECT_EQ(numItems, this->vbucket->getNumItems(evictionPolicy));
    EXPECT_EQ(numItems, this->vbucket->getHighSeqno());

    /* Now do a range read */
    std::pair<ENGINE_ERROR_CODE, std::vector<queued_item>> res =
                        this->vbucket->inMemoryBackfill(numItems + 1, numItems);

    EXPECT_EQ(ENGINE_ERANGE, res.first);
    std::vector<queued_item>& items = res.second;
    EXPECT_EQ(0, items.size());

    /* Free all items */
    items.clear();
}