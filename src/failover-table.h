/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
#ifndef SRC_FAILOVER_TABLE_H_
#define SRC_FAILOVER_TABLE_H_ 1

#include <deque>
#include <list>
#include <string>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include <platform/random.h>
#include "cJSON.h"

typedef struct {
    uint64_t vb_uuid;
    uint64_t by_seqno;
} failover_entry_t;

class FailoverTable {
 public:
    typedef std::list<failover_entry_t> table_t;

    FailoverTable(size_t capacity);
    FailoverTable();

    ~FailoverTable();

    uint64_t generateId();

    // Call when taking over as master to update failover table.
    // id should be generated to be fairly likely to be unique.
    void createEntry(uint64_t id, uint64_t high_sequence);

    // Where should client roll back to?
    uint64_t findRollbackPoint(uint64_t failover_id);

    // Client should be rolled back?
    bool needsRollback(uint64_t since, uint64_t failover_id);
    // Prune entries above seq (Should call this any time we roll back!)
    void pruneAbove(uint64_t seq);

    std::string toJSON();

    bool loadFromJSON(const std::string& json);

    table_t table;
    size_t max_entries;

 private:
    Couchbase::RandomGenerator provider;
    bool JSONtoEntry(cJSON* jobj, failover_entry_t& entry);

    DISALLOW_COPY_AND_ASSIGN(FailoverTable);
};

#endif
