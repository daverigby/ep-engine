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
#pragma once

/**
 * Ephemeral Bucket
 *
 * A bucket type without any persistent data storage. Similar to memcache (default)
 * buckets, except with VBucket goodness - replicaiton, rebalance, failover.
 */

#include "ep.h"

/* Note: this is currently a subclass of EPBucket, although it logically
 *       should be a peer. This is to facilitate incremental development -
 *       initially it starts identical to EPBucket, and we will gradually
 *       move functionality which should be common to both to the parent class
 *       (KVBucket), and functionality specific to Ephemeral will be added to
 *       this class.
 */
class EphemeralBucket : public EPBucket {
public:
    EphemeralBucket(EventuallyPersistentEngine& theEngine);
};
