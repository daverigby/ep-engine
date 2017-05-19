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
    leveldb::Slice v(mkValSlice(itm.getFlags(), itm.getExptime(),
                                itm.getNBytes(), itm.getData()));
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

leveldb::Slice LevelDBKVStore::mkValSlice(uint32_t flags, uint32_t exp,
                                          size_t n, const void *p) {
    adjustValBuffer(n);
    std::memcpy(valBuffer, &flags, sizeof(flags));
    std::memcpy(valBuffer + sizeof(flags), &exp, sizeof(exp));
    std::memcpy(valBuffer + sizeof(flags) + sizeof(exp), p, n);
    return leveldb::Slice(valBuffer, sizeof(flags) + sizeof(exp) + n);
}

void LevelDBKVStore::grokValSlice(const leveldb::Slice &s, uint32_t *f, uint32_t *e,
                                  size_t *sz, const char **p) {
    assert(s.size() >= 2 * sizeof(uint32_t));
    std::memcpy(f, s.data(), sizeof(*f));
    std::memcpy(e, s.data() + sizeof(*f), sizeof(*f));
    size_t data_size(s.size() - (sizeof(*f) + sizeof(*e)));
    std::memcpy(sz, &data_size, sizeof(*sz));
    *p = s.data() + sizeof(*f) + sizeof(*e);
}

GetValue LevelDBKVStore::makeGetValue(uint16_t vb,
                                      const DocKey& key,
                                      const std::string& value) {
    uint32_t flags, exp;
    size_t sz;
    const char *p;
    leveldb::Slice sval(value);
    grokValSlice(sval, &flags, &exp, &sz, &p);

    uint8_t ext_meta[EXT_META_LEN];
    if (checkUTF8JSON((const unsigned char *)p, sz)) {
        ext_meta[0] = PROTOCOL_BINARY_DATATYPE_JSON;
    } else {
        ext_meta[0] = PROTOCOL_BINARY_RAW_BYTES;
    }

    // TODO vmx 2016-10-29: put in real sequence number
    uint64_t seqno = 1;
    return GetValue(new Item(key,
                             flags,
                             exp,
                             p,
                             sz,
                             ext_meta,
                             EXT_META_LEN,
                             0, // CAS
                             seqno,
                             vb),
                    ENGINE_SUCCESS,
                    -1,
                    0);
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
