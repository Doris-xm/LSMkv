#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "Disk_store.h"
#define DATA_PATH "../data/"
#define FILE_PREFIX "level-"


class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    SkipList* memtable;
    SkipList* sstable;
    std::string dir;
    const int MAX_SIZE = 2097152;
    vector<int> level_size; //每层大小
    int level_num; //最大层数
    uint64_t timestamp;
    DiskStore* disk_store;

public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
private:
    void write_to_L0();
    void init();
    void dump();


};
