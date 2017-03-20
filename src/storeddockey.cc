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

#include <iostream>

#include "storeddockey.h"

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key) {
    return os << "ns:" << int(key.getDocNamespace()) << " " << key.c_str();
}

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key) {
    os << "ns:" << int(key.getDocNamespace()) << " ";
    for (size_t ii = 0; ii < key.size(); ++ii) {
        os << static_cast<char>(key.data()[ii]);
    }
    return os;
}
