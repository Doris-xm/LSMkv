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
            max_num(max_num), mode(mode), sstable_num(0),Serial(0) {}
    ~DiskLevel(){}
    uint64_t add_sstable(SSTable& sstable) {
        sstable_list.push_back(sstable);
        sstable.set_serial(Serial);
        sstable_num++;
        return Serial++;
    }
private:
    uint64_t Serial;

};

class DiskStore {
    uint32_t level_num;
    vector<DiskLevel> level_list;
    string read_file(const string& file_name, uint32_t offset, uint32_t len)const;
public:
    DiskStore();
    ~DiskStore() { }
    uint32_t get_level_num()const {
        return level_num;
    }
    void add_level( DiskLevel::LEVEL_MODE mode);
    uint64_t add_sstable(SSTable& sstable, uint32_t level);
    void put(uint64_t key, const std::string &s);
    std::string get(const uint64_t key)const;
    bool del(uint64_t key);
    void reset();
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list);
};

#endif //LSMKV_DISK_STORE_H
