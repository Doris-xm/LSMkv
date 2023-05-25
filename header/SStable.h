#ifndef LSMKV__SSTABLE_H_
#define LSMKV__SSTABLE_H_
#pragma once
#include <cstdio>
#include <vector>
#include <fstream>
#include <bitset>
#include <string>
#include "SkipList.h"
#include "utils.h"
#include "MurmurHash3.h"

//void dump_info(const std::string &file_name, const BuffTable &buff_table, const DataZone &data_zone);
//void split_file_name(const std::string &file,
//                     uint64_t &n_time_stamp,
//                     uint64_t &n_tag);
//void split_file_path(const std::string &file_path, uint64_t &level, uint64_t &time_stamp, uint64_t &tag);

const uint32_t BLOOM_SIZE = 81920;
const uint64_t HEADER_BYTE_SIZE = 10272;
const uint64_t OVERFLOW_SIZE = 2097152;
static uint64_t TIME_STAMP = 0;


class BloomFilter
{
    int Bit[BLOOM_SIZE];
public:
    BloomFilter() {
        for(int i = 0;i < BLOOM_SIZE; ++i)
            Bit[i] = 0;
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
};
struct Header {
    uint64_t time_stamp; //时间戳
    uint64_t total_num; //总的键值对
    uint64_t max_key; //最大的key
    uint64_t min_key; //最小的key
    Header():time_stamp(0), total_num(0), max_key(0), min_key(0) {}
    Header(uint64_t t_s, uint64_t num, uint64_t Max, uint64_t Min):
        time_stamp(t_s), total_num(num), max_key(Max), min_key(Min) {}
};

struct Indexer {
    uint64_t key;
    uint32_t offset;
    Indexer() = default;
    Indexer(uint64_t k, uint32_t o): key(k), offset(o) {}
};
class SSTable {
    Header* header;
    BloomFilter* bloom_filter;
    vector<Indexer> index_area;
    vector<string> data_area;
public:
    SSTable();
    ~SSTable();
    SSTable(SkipList *skip_list, uint64_t &time_stamp);
    string get(uint64_t key) const;
    bool find(uint64_t key) const{
        return bloom_filter->find(key);
    }
    uint64_t get_min_key() const {
        return header->min_key;
    }
    uint64_t get_max_key() const {
        return header->max_key;
    }

};

#endif //LSMKV__SSTABLE_H_