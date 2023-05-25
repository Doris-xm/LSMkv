#pragma once

#include <vector>
#include <climits>
#include <fstream>
#include <bitset>
#include <list>
#include "MurmurHash3.h"
using namespace std;

struct node{
    uint64_t key;
    string s;
    int level;
    vector<node*> point_list;
    node(int k = 0,string S = "",node* n = nullptr,int l = 0):key(k),level(l),s(S) {
        point_list.push_back(n);
    }
};

class SkipList {
private:
    int Max_level;
    node* head;
    node* tail;
    int P;
    unsigned int key_num;
    uint64_t max_key;
    uint64_t min_key;
    int max_level;
    void clear(node *&h);

public:
    std::bitset<81920> bits;
    uint64_t Size;
    SkipList();
    ~SkipList();
    std::string insert(uint64_t key, const std::string &s);
    std::string search(uint64_t key);
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list);
    void store(const string &path, uint64_t timestamp);
    node* get_head() { return head; }
    node* get_tail() { return tail; }
    unsigned int get_key_num() const { return key_num; }
    uint64_t get_max_key() const { return max_key; }
    uint64_t get_min_key() const { return min_key; }
};

