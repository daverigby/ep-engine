/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "evp_store_test.h"

#include "fakes/fake_executorpool.h"
#include "taskqueue.h"

/*
 * A subclass of EventuallyPersistentStoreTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedEPStoreTest : public EventuallyPersistentStoreTest {
    void SetUp() {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        EventuallyPersistentStoreTest::SetUp();
    }
};

/* Regression / reproducer test for MB-19695 - an exception is thrown
 * (and connection disconnected) if a couchstore file hasn't been re-created
 * yet when doTapVbTakeoverStats() is called as part of
 * tapNotify / TAP_OPAQUE_INITIAL_VBUCKET_STREAM.
 */
TEST_F(SingleThreadedEPStoreTest, MB19695_doTapVbTakeoverStats) {
    auto* task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
        (ExecutorPool::get());

    // Should start with no tasks registered on any queues.
    for (auto& queue : task_executor->getLpTaskQ()) {
        ASSERT_EQ(0, queue->getFutureQueueSize());
        ASSERT_EQ(0, queue->getReadyQueueSize());
    }

    // [[1] Set our state to replica. This should add a VBStatePersistTask to
    // the WRITER queue.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_replica, false));

    auto& lpWriterQ = task_executor->getLpTaskQ()[WRITER_TASK_IDX];
    auto& lpNonioQ = task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    EXPECT_EQ(1, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());

    // Use a FakeExecutorThread to fetch and run the persistTask.
    FakeExecutorThread fake_thread(task_executor, WRITER_TASK_IDX);
    EXPECT_TRUE(lpWriterQ->fetchNextTask(fake_thread, false));
    EXPECT_EQ("Persisting a vbucket state for vbucket: 0",
              fake_thread.getTaskName());
    EXPECT_EQ(0, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());
    fake_thread.runCurrentTask();

    // [[2]] Perform a vbucket reset. This will perform some work synchronously,
    // but also schedules 3 tasks:
    //   1. vbucket memory deletion (NONIO)
    //   2. vbucket disk deletion (WRITER)
    //   3. VBStatePersistTask (WRITER)
    // MB-19695: If we try to get the number of persisted deletes between
    // tasks (2) and (3) running then an exception is thrown (and client
    // disconnected).
    EXPECT_TRUE(store->resetVBucket(vbid));

    EXPECT_EQ(2, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());
    EXPECT_EQ(1, lpNonioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpNonioQ->getReadyQueueSize());

    // Fetch and run first two tasks.
    // Should have just one task on NONIO queue - "Delete vBucket (memory)"
    fake_thread.updateCurrentTime();
    EXPECT_TRUE(lpNonioQ->fetchNextTask(fake_thread, false));
    EXPECT_EQ(0, lpNonioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpNonioQ->getReadyQueueSize());
    EXPECT_EQ("Removing (dead) vbucket 0 from memory",
              fake_thread.getTaskName());
    fake_thread.runCurrentTask();

    // Run vbucket deletion task (so there is no file on disk).
    fake_thread.updateCurrentTime();
    EXPECT_TRUE(lpWriterQ->fetchNextTask(fake_thread, false));
    EXPECT_EQ("Deleting VBucket:0", fake_thread.getTaskName());
    fake_thread.runCurrentTask();

    // [[2]] Ok, let's see if we can get TAP takeover stats. This will
    // fail with MB-19695.
    // Dummy callback to passs into the stats function below.
    auto dummy_cb = [](const char *key, const uint16_t klen,
                          const char *val, const uint32_t vlen,
                          const void *cookie) {};
    std::string key{"MB19695_doTapVbTakeoverStats"};
    EXPECT_NO_THROW(engine->public_doTapVbTakeoverStats
                    (nullptr, dummy_cb, key, vbid));

    // Cleanup - run the 3rd task - VBStatePersistTask.
    fake_thread.updateCurrentTime();
    EXPECT_TRUE(lpWriterQ->fetchNextTask(fake_thread, false));
    EXPECT_EQ("Persisting a vbucket state for vbucket: 0",
              fake_thread.getTaskName());
    fake_thread.runCurrentTask();
}

// Check that if onDeleteItem() is called during bucket deletion, we do not
// abort due to not having a valid thread-local 'engine' pointer. This
// has been observed when we have a DCPBackfill task which is deleted during
// bucket shutdown, which has a non-zero number of Items which are destructed
// (and call onDeleteItem).
TEST_F(SingleThreadedEPStoreTest, MB20054_onDeleteItem_during_bucket_deletion) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Perform one SET, then close it's checkpoint. This means that we no
    // longer have all sequence numbers in memory checkpoints, forcing the
    // DCP stream request to go to disk (backfill).
    store_item(vbid, "key", "value");

    // Force a new checkpoint.
    auto vb = store->getVbMap().getBucket(vbid);
    auto& ckpt_mgr = vb->checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    // Directly flush the vbucket, ensuring data is on disk.
    //  (This would normally also wake up the checkpoint remover task, but
    //   as that task was never registered with the ExecutorPool in this test
    //   environment, we need to manually remove the prev checkpoint).
    EXPECT_EQ(1, store->flushVBucket(vbid));

    bool new_ckpt_created;
    EXPECT_EQ(1,
              ckpt_mgr.removeClosedUnrefCheckpoints(vb, new_ckpt_created));

    // Create a DCP producer, and start a stream request.
    std::string name{"test_producer"};
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcpOpen(cookie, /*opaque:unused*/{}, /*seqno:unused*/{},
                              DCP_OPEN_PRODUCER, name.data(), name.size()));

    uint64_t rollbackSeqno;
    auto dummy_dcp_add_failover_cb = [](vbucket_failover_t* entry,
                                       size_t nentries, const void *cookie) {
        return ENGINE_SUCCESS;
    };

    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine.get()->dcp.stream_req(
                      &engine.get()->interface, cookie, /*flags*/0,
                      /*opaque*/0, /*vbucket*/vbid, /*start_seqno*/0,
                      /*end_seqno*/-1, /*vb_uuid*/0xabcd, /*snap_start*/0,
                      /*snap_end*/0, &rollbackSeqno,
                      dummy_dcp_add_failover_cb));

    // Run the DCPBackfill task (5 times?), so add some items to the
    // stream readyQ
    auto* task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
        (ExecutorPool::get());
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];

    // Create a CheckExeckedExecutor 'thread' to obtain shared ownership of
    // the next AuxIO task (which should be DCPBackfill). As long as this
    // object is in scope, the DCPBackfilltask will not be deleted.
    // Essentially we are simulating a concurrent thread running this task.
    CheckedExecutor executor(task_executor, lpAuxioQ);
    EXPECT_EQ("Backfilling items for a DCP Connection",
              executor.getTaskName());


//    // First time it runs we push one item to readyQ (snapshot marker)
//    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // Now trigger deletion of the bucket, *while* the DCPBackfill task is
    // still 'running' (via CheckedExecutor).
    engine->destroy(/*force*/false);

//    // Run the next task (another DCPBackfill).
//    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
//
//    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
//    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
//
//    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
//
    // Trigger deletion of the bucket (engine).
//    engine.reset();
}
