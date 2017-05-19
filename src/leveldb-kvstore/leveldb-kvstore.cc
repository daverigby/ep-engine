/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

#include <ep_engine.h>
#include <JSON_checker.h>
#include "leveldb-kvstore.hh"
#include "vbucket.h"

static const size_t DEFAULT_VAL_SIZE(64 * 1024);

LevelDBKVStore::LevelDBKVStore(KVStoreConfig &config)
    : KVStore(config),
      valBuffer(NULL),
      valSize(0),
      batch(NULL),
      scanCounter(0) {
    keyBuffer = static_cast<char*>(calloc(1, sizeof(uint16_t)
                                          + std::numeric_limits<uint8_t>::max()));
    cachedVBStates.reserve(configuration.getMaxVBuckets());
    cachedVBStates.assign(configuration.getMaxVBuckets(), nullptr);

    adjustValBuffer(DEFAULT_VAL_SIZE);
    open();
}

void LevelDBKVStore::adjustValBuffer(const size_t to) {
    // Save room for the flags, exp, etc...
    size_t needed((sizeof(uint32_t)*2) + to);

    if (valBuffer == NULL || valSize < needed) {
        void *buf = realloc(valBuffer, needed);
        if (buf) {
            valBuffer = static_cast<char*>(buf);
            valSize = needed;
        }
    }
}

std::vector<vbucket_state *> LevelDBKVStore::listPersistedVbuckets() {
    // TODO:  Something useful.
    //std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    std::vector<vbucket_state *> rv;
    return rv;
}

void LevelDBKVStore::set(const Item &itm, Callback<mutation_result> &cb) {
    // TODO-PERF: See if slices can be setup without copying.
    leveldb::Slice k(mkKeySlice(itm.getVBucketId(), itm.getKey()));
    leveldb::Slice v(mkValSlice(itm));
    batch->Put(k, v);
    std::pair<int, bool> p(1, true);
    cb.callback(p);
}


void LevelDBKVStore::get(const DocKey& key, uint16_t vb,
                         Callback<GetValue> &cb, bool fetchDelete) {
    getWithHeader(nullptr, key, vb, cb, fetchDelete);
}


void LevelDBKVStore::getWithHeader(void* handle, const DocKey& key,
                                  uint16_t vb, Callback<GetValue>& cb,
                                  bool fetchDelete) {
    leveldb::Slice k(mkKeySlice(vb, key));
    std::string value;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), k, &value);
    if (!s.ok()) {
        GetValue rv(NULL, ENGINE_KEY_ENOENT);
        cb.callback(rv);
    }
    GetValue rv = makeGetValue(vb, key, value);
    cb.callback(rv);
}

// PERF: LevelDB doesn't support multi-get (rocks does), so we just emulate
// with individual Get() calls.
void LevelDBKVStore::getMulti(uint16_t vb, vb_bgfetch_queue_t &itms) {
    for (auto& it : itms) {
        auto& key = it.first;
        leveldb::Slice vbAndKey(mkKeySlice(vb, it.first));
        std::string value;
        leveldb::Status s = db->Get(leveldb::ReadOptions(), vbAndKey, &value);
        if (s.ok()) {
            GetValue rv = makeGetValue(vb, key, value);
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value = rv;
            }
        } else {
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value.setStatus(ENGINE_KEY_ENOENT);
            }
        }
    }
}

void LevelDBKVStore::reset(uint16_t vbucketId) {
    if (db) {
        // TODO:  Implement.
    }
}

void LevelDBKVStore::del(const Item &itm, Callback<int> &cb) {
    leveldb::Slice k(mkKeySlice(itm.getVBucketId(), itm.getKey()));
    batch->Delete(k);
    int rv(1);
    cb.callback(rv);
}

static bool matches_prefix(leveldb::Slice s, size_t len, const char *p) {
    return s.size() >= len && std::memcmp(p, s.data(), len) == 0;
}

void LevelDBKVStore::delVBucket(uint16_t vb, uint64_t vb_version) {
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    const char *prefix(reinterpret_cast<const char*>(&vb));
    std::string start(prefix, sizeof(vb));
    begin();
    for (it->Seek(start);
         it->Valid() && matches_prefix(it->key(), sizeof(vb), prefix);
         it->Next()) {
        batch->Delete(it->key());
    }
    delete it;
    commit(nullptr); // TODO: pass non-null for collections manifest.
}

bool LevelDBKVStore::snapshotVBucket(uint16_t vbucketId, const vbucket_state &vbstate,
                                     VBStatePersist options) {
    // TODO:  Implement
    return true;
}

bool LevelDBKVStore::snapshotStats(const std::map<std::string, std::string> &) {
    // TODO:  Implement
    return true;
}

void LevelDBKVStore::destroyInvalidVBuckets(bool) {
    // TODO:  implement
}

StorageProperties LevelDBKVStore::getStorageProperties(void) {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

leveldb::Slice LevelDBKVStore::mkKeySlice(uint16_t vbid, const DocKey& k) {
    std::memcpy(keyBuffer, &vbid, sizeof(vbid));
    std::memcpy(keyBuffer + sizeof(vbid), k.data(), k.size());
    return leveldb::Slice(keyBuffer, sizeof(vbid) + k.size());
}

void LevelDBKVStore::grokKeySlice(const leveldb::Slice &s, uint16_t *v, std::string *k) {
    assert(s.size() > sizeof(uint16_t));
    std::memcpy(v, s.data(), sizeof(uint16_t));
    k->assign(s.data() + sizeof(uint16_t), s.size() - sizeof(uint16_t));
}

leveldb::Slice LevelDBKVStore::mkValSlice(const Item& item) {
    // Serialize an Item to the format to write to LevelDB.
    // Using the following layout:
    //    uint64_t           cas          ]
    //    uint64_t           revSeqno     ] ItemMetaData
    //    uint32_t           flags        ]
    //    uint32_t           exptime      ]
    //    uint64_t           bySeqno
    //    uint8_t            datatype
    //    uint32_t           value_len
    //    uint8_t[value_len] value
    //
    adjustValBuffer(item.size());
    char* dest = valBuffer;
    std::memcpy(dest, &item.getMetaData(), sizeof(ItemMetaData));
    dest += sizeof(ItemMetaData);

    const int64_t bySeqno{item.getBySeqno()};
    std::memcpy(dest, &bySeqno, sizeof(bySeqno));
    dest += sizeof(uint64_t);

    const uint8_t datatype{item.getDataType()};
    std::memcpy(dest, &datatype, sizeof(datatype));
    dest += sizeof(uint8_t);

    const uint32_t valueLen = item.getNBytes();
    std::memcpy(dest, &valueLen, sizeof(valueLen));
    dest += sizeof(valueLen);
    std::memcpy(dest, item.getValue()->getData(), valueLen);
    dest += valueLen;

    return leveldb::Slice(valBuffer, dest - valBuffer);
}

Item* LevelDBKVStore::grokValSlice(uint16_t vb,
                                  const DocKey& key,
                                  const leveldb::Slice& s) {
    // Reverse of mkValSlice - deserialize back into an Item.

    assert(s.size() >= sizeof(ItemMetaData) + sizeof(uint64_t) +
                               sizeof(uint8_t) + sizeof(uint32_t));

    ItemMetaData meta;
    const char* src = s.data();
    std::memcpy(&meta, src, sizeof(meta));
    src += sizeof(meta);

    int64_t bySeqno;
    std::memcpy(&bySeqno, src, sizeof(bySeqno));
    src += sizeof(bySeqno);

    uint8_t datatype;
    std::memcpy(&datatype, src, sizeof(datatype));
    src += sizeof(datatype);

    uint32_t valueLen;
    std::memcpy(&valueLen, src, sizeof(valueLen));
    src += sizeof(valueLen);

    uint8_t extMeta[EXT_META_LEN];
    extMeta[0] = datatype;

    return new Item(key,
                meta.flags,
                meta.exptime,
                src,
                valueLen,
                extMeta,
                EXT_META_LEN,
                meta.cas,
                bySeqno,
                vb,
                meta.revSeqno);
}

GetValue LevelDBKVStore::makeGetValue(uint16_t vb,
                                      const DocKey& key,
                                      const std::string& value) {
    leveldb::Slice sval(value);
    return GetValue(grokValSlice(vb, key, sval), ENGINE_SUCCESS, -1, 0);
}

ScanContext* LevelDBKVStore::initScanContext(
    std::shared_ptr<Callback<GetValue> > cb,
    std::shared_ptr<Callback<CacheLookup> > cl,
    uint16_t vbid, uint64_t startSeqno,
    DocumentFilter options,
    ValueFilter valOptions) {
    // TODO vmx 2016-10-29: implement
    size_t scanId = scanCounter++;
    return new ScanContext(cb, cl, vbid, scanId, startSeqno,
                           99999999, options,
                           valOptions, 999999, configuration);
}
