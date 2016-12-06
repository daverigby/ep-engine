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

#include "ep_bucket.h"

#include "ep_engine.h"
#include "warmup.h"

#include <platform/make_unique.h>


EPBucket::EPBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine) {
}

bool EPBucket::initialize() {
    KVBucket::initialize();

    if (!startFlusher()) {
        LOG(EXTENSION_LOG_WARNING,
            "FATAL: Failed to create and start flushers");
        return false;
    }
    return true;
}

void EPBucket::deinitialize() {
    stopFlusher();

    KVBucket::deinitialize();
}
