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

#include "ep_types.h"
#include "executorpool.h"
#include "stored-value.h"
#include "task_type.h"
#include "vbucket.h"
#include "vbucketmap.h"
#include "utility.h"
#include "kvbucket.h"
#include "ep.h"

class EphemeralBucket : public EPBucket {
public:
    EphemeralBucket(EventuallyPersistentEngine& theEngine);

    ENGINE_ERROR_CODE setVBucketState_UNLOCKED(uint16_t vbid,
                                               vbucket_state_t to,
                                               bool transfer,
                                               bool notify_dcp,
                                               LockHolder& vbset);
    DISALLOW_COPY_AND_ASSIGN(EphemeralBucket);
};