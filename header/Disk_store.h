#pragma once
#include "SStable.h"
#include "Constant.h"
using namespace std;


struct DiskLevel {
    enum LEVEL_MODE {
        TIERING,
        LEVELING,
    };
    uint64_t sstable_num;
    uint64_t max_num;
    vector<SSTable*> sstable_list;
    LEVEL_MODE mode;

    DiskLevel(uint64_t max_num, LEVEL_MODE mode):
            max_num(max_num), mode(mode), sstable_num(0),Serial(0) {}
    ~DiskLevel(){
        for(SSTable* sstable: sstable_list) {
            delete sstable;
        }
    }
    uint64_t add_sstable(SSTable* sstable) {
        sstable_list.push_back(sstable);
        sstable->set_serial(Serial);
        sstable_num++;
        return Serial++;
    }
    void choose_sstables(vector<SSTable*> &sstable_list, uint64_t min_key, uint64_t max_key,int mode);
private:
    uint64_t Serial;

};

class DiskStore {
    vector<pair<uint32_t,DiskLevel::LEVEL_MODE>> config;
    uint32_t level_num;
    vector<DiskLevel*> level_list;
    string read_file(const string& file_name, uint32_t offset, int len)const;
    bool check_level_overflow(uint32_t level)const;
    void mergeSort(vector<pair<uint64_t,string>> data1, vector<pair<uint64_t,string>> data2, vector<pair<uint64_t,string>> &data_sorted);
public:
    DiskStore(const string &config_dir);
    DiskStore(){}
    ~DiskStore() { }
    uint32_t get_level_num()const {
        return level_num;
    }
    void add_level( DiskLevel::LEVEL_MODE mode);
    uint64_t add_sstable(SSTable* sstable, uint32_t level,bool &flag);
    std::string get(const uint64_t key,const string & dir_prefix)const;

    void compaction(uint32_t dump_to_level, const string & dir);
};


