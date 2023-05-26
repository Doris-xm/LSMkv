#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "Disk_store.h"

#define DELETE_VAL "~DELETED~"
#define CONFIG_DIR "../config/config.txt"
extern string DATA_PATH;
extern string FILE_PREFIX;
/*
 * @brief KVStore
 * @details 对外接口的实现，控制time stamp的更新
 *          调用SkipList的接口，实现memtable存储
 *          调用DiskStore的接口，磁盘中每层的控制，存储sstable
 * */
class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    SkipList* memtable;
    uint64_t timestamp;
    DiskStore* disk_store;

public:
    KVStore(const std::string &dir = "../data");

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
private:
    void init();
    void dump();
    void compaction(uint32_t dump_to_level);


};
