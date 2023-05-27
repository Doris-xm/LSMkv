#pragma once

#include "skiplist.h"
#include "../utils.h"
#include "MurmurHash3.h"
#include "Constant.h"
#include <bitset>

//const uint32_t BLOOM_SIZE = 81920;
//const uint64_t HEADER_BYTE_SIZE = 10272;
//const uint64_t TWO_MB = 2097152;



class BloomFilter
{
    int Bit[BLOOM_SIZE];
public:
    BloomFilter() {
        for(int i = 0;i < BLOOM_SIZE; ++i)
            Bit[i] = 0;
    }
    BloomFilter( bitset<BLOOM_SIZE> &bit) {
        for(int i = 0;i < BLOOM_SIZE; ++i)
            Bit[i] = bit[i];
    }
    ~BloomFilter() {}
    void insert(uint64_t key) {
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(unsigned int i = 0; i < 4; ++i )
            Bit[i % BLOOM_SIZE] = 1;
    }
    bool find(uint64_t key) const {
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(unsigned int i = 0; i < 4; ++i ) {
            if (!Bit[i % BLOOM_SIZE])
                return false;
        }
        return true;
    }
    void getBit(bitset<BLOOM_SIZE> &bitset){
        for(int i = 0;i < BLOOM_SIZE; ++i)
            bitset[i] = Bit[i];
    }
};
struct Header {
    uint64_t time_stamp; //时间戳
    uint64_t total_num; //总的键值对
    uint64_t max_key; //最大的key
    uint64_t min_key; //最小的key
    Header():time_stamp(0), total_num(0), max_key(0), min_key(0) {}
    Header(uint64_t t_s, uint64_t num, uint64_t Max, uint64_t Min):
        time_stamp(t_s), total_num(num), max_key(Max), min_key(Min) {}
    ~Header() {}
};

struct Indexer {
    uint64_t key;
    uint32_t offset;
    Indexer() { };
    Indexer(uint64_t k, uint32_t o): key(k), offset(o) {}
};
class SSTable {
    Header* header;
    BloomFilter* bloom_filter;
    vector<Indexer> index_area;
    vector<string> data_area; //dump的时候暂存数据
    uint64_t Serial; //序列号,用于区分同一时间戳的SSTable（文件命名）

public:
    SSTable();
    ~SSTable();
    SSTable(SkipList *skip_list, uint64_t time_stamp);
    SSTable(const vector<pair<uint64_t,string>>& data, const uint64_t time_stamp);
    SSTable(const string &file_path, uint64_t time_stamp,uint64_t serial);
    bool get(uint64_t key, uint32_t & offset, int & size);
    bool find(uint64_t key) const{
        return bloom_filter->find(key);
    }
    uint64_t get_min_key() const {
        return header->min_key;
    }
    uint64_t get_max_key() const {
        return header->max_key;
    }
    uint64_t get_total_num() const {
        return header->total_num;
    }
    void save_file(const string &file_name);
    void set_serial(const uint64_t serial) {
        Serial = serial;
    }
    uint64_t get_serial() const {
        return Serial;
    }
    uint64_t get_time_stamp() const {
        return header->time_stamp;
    }
    void read_to_mem(const string &file_path,vector< pair<uint64_t, string> > &data);

private:
    int binary_search(uint64_t key) const;
//    string read_file(const uint32_t offset, const uint32_t size) const;
//    string make_file_name();

};
