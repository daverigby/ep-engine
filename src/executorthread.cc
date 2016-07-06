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

#include "config.h"

#include <queue>

#include "common.h"
#include "executorpool.h"
#include "executorthread.h"
#include "taskqueue.h"
#include "ep_engine.h"

AtomicValue<size_t> GlobalTask::task_id_counter(1);

extern "C" {
    static void launch_executor_thread(void *arg) {
        ExecutorThread *executor = (ExecutorThread*) arg;
        try {
            executor->run();
        } catch (std::exception& e) {
            LOG(EXTENSION_LOG_WARNING, "%s: Caught an exception: %s\n",
                executor->getName().c_str(), e.what());
        } catch(...) {
            LOG(EXTENSION_LOG_WARNING, "%s: Caught a fatal exception\n",
                executor->getName().c_str());
        }
    }
}

void ExecutorThread::start() {
    cb_assert(state == EXECUTOR_CREATING);
    if (cb_create_thread(&thread, launch_executor_thread, this, 0) != 0) {
        std::stringstream ss;
        ss << name.c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }
}

void ExecutorThread::stop(bool wait) {
    if (!wait && (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD)) {
        return;
    }
    state = EXECUTOR_SHUTDOWN;
    if (!wait) {
        LOG(EXTENSION_LOG_INFO, "%s: Stopping", name.c_str());
        return;
    }
    cb_join_thread(thread);
    LOG(EXTENSION_LOG_INFO, "%s: Stopped", name.c_str());
}

void ExecutorThread::run() {
    state = EXECUTOR_RUNNING;

    LOG(EXTENSION_LOG_DEBUG, "Thread %s running..", getName().c_str());

    for (uint8_t tick = 1;; tick++) {
        {
            LockHolder lh(currentTaskMutex);
            currentTask.reset();
        }
        if (state != EXECUTOR_RUNNING) {
            break;
        }

        now = gethrtime();
        if (TaskQueue *q = manager->nextTask(*this, tick)) {
            EventuallyPersistentEngine *engine = currentTask->getEngine();
            ObjectRegistry::onSwitchThread(engine);
            if (currentTask->isdead()) {
                // release capacity back to TaskQueue
                manager->doneWork(curTaskType);
                manager->cancel(currentTask->taskId, true);
                continue;
            }

            // Measure scheduling overhead as difference between the time
            // that the task wanted to wake up and the current time
            hrtime_t woketime = currentTask->getWaketime();
            engine->getEpStore()->logQTime(currentTask->getTypeId(),
                                           now > woketime ? now - woketime
                                                          : 0);

            taskStart = now;
            rel_time_t startReltime = ep_current_time();
            try {
                LOG(EXTENSION_LOG_DEBUG,
                    "%s: Run task \"%s\" id %d",
                getName().c_str(), currentTask->getDescription().c_str(),
                currentTask->getId());

                // Now Run the Task ....
                currentTask->setState(TASK_RUNNING, TASK_SNOOZED);
                bool again = currentTask->run();

                // Task done, log it ...
                hrtime_t runtime((gethrtime() - taskStart) / 1000);
                engine->getEpStore()->logRunTime(currentTask->getTypeId(),
                                               runtime);
                ObjectRegistry::onSwitchThread(NULL);
                addLogEntry(engine->getName() + currentTask->getDescription(),
                        q->getQueueType(), runtime, startReltime,
                        (runtime >
                         (hrtime_t)currentTask->maxExpectedDuration()));
                ObjectRegistry::onSwitchThread(engine);
                // Check if task is run once or needs to be rescheduled..
                if (!again || currentTask->isdead()) {
                    // release capacity back to TaskQueue
                    manager->doneWork(curTaskType);
                    manager->cancel(currentTask->taskId, true);
                } else {
                    hrtime_t new_waketime;
                    // if a task has not set snooze, update its waketime to now
                    // before rescheduling for more accurate timing histograms
                    currentTask->updateWaketimeIfLessThan(now);

                    // release capacity back to TaskQueue ..
                    manager->doneWork(curTaskType);
                    new_waketime = q->reschedule(currentTask, curTaskType);
                    // record min waketime ...
                    if (new_waketime < waketime) {
                        waketime = new_waketime;
                    }
                    LOG(EXTENSION_LOG_DEBUG, "%s: Reschedule a task"
                            " \"%s\" id %d[%llu %llu |%llu]",
                            name.c_str(),
                            currentTask->getDescription().c_str(),
                            currentTask->getId(), new_waketime,
                            currentTask->getWaketime(),
                            waketime.load());
                }
            } catch (std::exception& e) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Exception caught in task \"%s\": %s", name.c_str(),
                    currentTask->getDescription().c_str(), e.what());
            } catch(...) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Fatal exception caught in task \"%s\"\n",
                    name.c_str(), currentTask->getDescription().c_str());
            }
        }
    }
    state = EXECUTOR_DEAD;
}

void ExecutorThread::setCurrentTask(ExTask newTask) {
    LockHolder lh(currentTaskMutex);
    currentTask = newTask;
}

void ExecutorThread::addLogEntry(const std::string &desc,
                                 const task_type_t taskType,
                                 const hrtime_t runtime,
                                 rel_time_t t, bool isSlowJob) {
    LockHolder lh(logMutex);
    TaskLogEntry tle(desc, taskType, runtime, t);
    if (isSlowJob) {
        slowjobs.add(tle);
    } else {
        tasklog.add(tle);
    }
}

const std::string ExecutorThread::getStateName() {
    switch (state.load()) {
    case EXECUTOR_CREATING:
        return std::string("creating");
    case EXECUTOR_RUNNING:
        return std::string("running");
    case EXECUTOR_WAITING:
        return std::string("waiting");
    case EXECUTOR_SLEEPING:
        return std::string("sleeping");
    case EXECUTOR_SHUTDOWN:
        return std::string("shutdown");
    default:
        return std::string("dead");
    }
}
