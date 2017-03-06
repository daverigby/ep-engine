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

#pragma once

/**
 * Factory for creating StoredValue objects.
 */

#include <memory>

#include "hash_table.h"
#include "stored-value.h"

/**
 * Abstract base class for StoredValue factories.
 */
class AbstractStoredValueFactory {
public:
    virtual ~AbstractStoredValueFactory() {}

    /**
     * Create a new StoredValue (or subclass) with the given item.
     *
     * @param itm the item the StoredValue should contain
     * @param next The StoredValue which will follow the new stored value in
     *             the hash bucket chain, which this new item will take
     *             ownership of. (Typically the top of the hash bucket into
     *             which the new item is being inserted).
     * @param ht the hashtable that will contain the StoredValue instance
     *           created
     */
    virtual StoredValue::UniquePtr operator()(const Item& itm,
                                              StoredValue::UniquePtr next,
                                              HashTable& ht) = 0;
};

/**
 * Creator of StoredValue instances.
 */
class StoredValueFactory : public AbstractStoredValueFactory {
public:
    using value_type = StoredValue;

    StoredValueFactory(EPStats& s) : stats(&s) {
    }

    /**
     * Create an concrete StoredValue object.
     */
    StoredValue::UniquePtr operator()(const Item& itm,
                                      StoredValue::UniquePtr next,
                                      HashTable& ht) override {
        // Allocate a buffer to store the StoredValue and any trailing bytes
        // that maybe required.
        return StoredValue::UniquePtr(
                new (::operator new(StoredValue::getRequiredStorage(itm)))
                        StoredValue(itm, std::move(next), *stats, ht));
    }

private:
    EPStats* stats;
};