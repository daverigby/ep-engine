/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LEVELDB_KVSTORE_H
#define LEVELDB_KVSTORE_H 1

#include <map>
#include <vector>

#include <kvstore.h>

#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <leveldb/write_batch.h>


/**
 * A persistence store based on leveldb.
 */
class LevelDBKVStore : public KVStore {
public:
    /**
     * Constructor
     *
     * @param config    Configuration information
     * @param read_only flag indicating if this kvstore instance is for read-only operations
     */
    LevelDBKVStore(KVStoreConfig &config);

    /**
     * Cleanup.
     */
    ~LevelDBKVStore() {
        close();
        free(keyBuffer);
        free(valBuffer);
    }

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId);

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin() {
        if(!batch) {
            batch = new leveldb::WriteBatch;
        }
        return batch != NULL;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(const Item* collectionsManifest) {
        if(batch) {
            leveldb::Status s = db->Write(leveldb::WriteOptions(), batch);
            if (s.ok()) {
                delete batch;
                batch = NULL;
            }
        }
        return batch == NULL;
    }

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() {
        if (batch) {
            delete batch;
        }
    }

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties();

    /**
     * Overrides set().
     */
    void set(const Item &item, Callback<mutation_result> &cb);

    /**
     * Overrides get().
     */
    void get(const DocKey& key, uint16_t vb, Callback<GetValue> &cb,
             bool fetchDelete = false);

    void getWithHeader(void* handle, const DocKey& key,
                       uint16_t vb, Callback<GetValue>& cb,
                       bool fetchDelete = false);

    void getMulti(uint16_t vb, vb_bgfetch_queue_t &itms);

    /**
     * Overrides del().
     */
    void del(const Item &itm, Callback<int> &cb);

    void delVBucket(uint16_t vbucket, uint64_t vb_version);

    std::vector<vbucket_state *> listPersistedVbuckets(void);

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBucket(uint16_t vbucketId, const vbucket_state &vbstate,
                         VBStatePersist options);

    void destroyInvalidVBuckets(bool);

    size_t getNumShards() {
        return 1;
    }

    //size_t getShardId(const QueuedItem &) {
    //    return 0;
    //}

    void optimizeWrites(std::vector<queued_item> &) {
    }

    uint16_t getNumVbsPerFile(void) override {
        // TODO vmx 2016-10-29: return the actual value
        return 1024;
    }

    bool compactDB(compaction_ctx *) {
        // Explicit compaction is not needed
        return true;
    }

    uint16_t getDBFileId(const protocol_binary_request_compact_db&) {
        // Not needed if there is no explicit compaction
        return 0;
    }

    vbucket_state * getVBucketState(uint16_t vbucketId) {
        // TODO vmx 2016-10-29: implement
        return cachedVBStates[vbucketId];
    }

    size_t getNumPersistedDeletes(uint16_t vbid) {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    DBFileInfo getDbFileInfo(uint16_t vbid) {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    DBFileInfo getAggrDbFileInfo() {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    size_t getItemCount(uint16_t vbid) {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    RollbackResult rollback(uint16_t vbid, uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) {
        // TODO vmx 2016-10-29: implement
        // NOTE vmx 2016-10-29: For LevelDB it will probably always be a
        // full rollback as it doesn't support Couchstore like rollback
        // semantics
        return RollbackResult(false, 0, 0, 0);
    }

    void pendingTasks() {
        // NOTE vmx 2016-10-29: Intentionally left empty;
    }

    ENGINE_ERROR_CODE getAllKeys(uint16_t vbid, const DocKey start_key,
                                 uint32_t count,
                                 std::shared_ptr<Callback<const DocKey&>> cb) {
        // TODO vmx 2016-10-29: implement
        return ENGINE_SUCCESS;
    }

    ScanContext* initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                 std::shared_ptr<Callback<CacheLookup> > cl,
                                 uint16_t vbid, uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions);

    scan_error_t scan(ScanContext* sctx) {
        // TODO vmx 2016-10-29: implement
        return scan_success;
    }

    void destroyScanContext(ScanContext* ctx) {
        // TODO vmx 2016-10-29: implement
        delete ctx;
    }

    bool persistCollectionsManifestItem(uint16_t vbid,
                                        const Item& manifestItem) {
        // TODO DJR 2017-05-19 implement this.
        return false;
    }

    std::string getCollectionsManifest(uint16_t vbid) {
        // TODO DJR 2017-05-19 implement this.
        return "";
    }

    void incrementRevision(uint16_t vbid) {
        // TODO DJR 2017-05-19 implement this.
    }

    uint64_t prepareToDelete(uint16_t vbid) {
        // TODO DJR 2017-05-19 implement this.
        return 0;
    }


private:

    /**
     * Direct access to the DB.
     */
    leveldb::DB* db;
    char *keyBuffer;
    char *valBuffer;
    size_t valSize;

    void open() {
        leveldb::Options options;
        options.create_if_missing = true;
        const std::string dbname = configuration.getDBName() + "." +
                             std::to_string(configuration.getShardId());

        leveldb::Status s = leveldb::DB::Open(options, dbname, &db);
        if (!s.ok()) {
            throw std::runtime_error(
                    "LevelDBKVStore::open: failed to open database '" + dbname +
                    "': " + s.ToString());
        }
    }

    void close() {
        delete db;
        db = NULL;
    }

    leveldb::Slice mkKeySlice(uint16_t, const DocKey& k);
    void grokKeySlice(const leveldb::Slice &, uint16_t *, std::string *);

    void adjustValBuffer(const size_t);

    leveldb::Slice mkValSlice(const Item& item);
    Item* grokValSlice(uint16_t vb, const DocKey& key, const leveldb::Slice& s);

    GetValue makeGetValue(uint16_t vb,
                          const DocKey& key,
                          const std::string& value);

    leveldb::WriteBatch *batch;

    // Disallow assignment.
    void operator=(LevelDBKVStore &from);

    std::atomic<size_t> scanCounter; //atomic counter for generating scan id
};

#endif /* LEVELDB_KVSTORE_H */
