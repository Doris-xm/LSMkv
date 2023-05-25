#ifndef LSMKV_DISK_STORE_H
#define LSMKV_DISK_STORE_H
#include "SStable.h"
using namespace std;

struct DiskLevel {
    enum LEVEL_MODE {
        TIERING,
        LEVELING,
    };
    uint64_t sstable_num;
    uint64_t max_num;
    vector<SSTable> sstable_list;
    LEVEL_MODE mode;
    DiskLevel(uint64_t max_num, LEVEL_MODE mode):
            max_num(max_num), mode(mode), sstable_num(0) {}
    ~DiskLevel(){}

};
class DiskStore {
    uint32_t level_num;
    vector<DiskLevel> level_list;
public:
    DiskStore();
    ~DiskStore() { }
    uint32_t get_level_num()const {
        return level_num;
    }
    void add_level( DiskLevel::LEVEL_MODE mode);
    void put(uint64_t key, const std::string &s);
    std::string get(uint64_t key);
    bool del(uint64_t key);
    void reset();
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list);
};

#endif //LSMKV_DISK_STORE_H
