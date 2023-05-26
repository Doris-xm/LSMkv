#include "../header/skiplist.h"

void SkipList::clear(node *&h) {
    if( !h ) return;
    clear(h->point_list[0]);
    delete h;
    h = nullptr;
}

SkipList::~SkipList() {
    clear(head);
}

SkipList::SkipList() {
    P = 368; // probability = 1/e
    tail = new node(INT_MAX);
    head = new node(INT_MIN,"",tail);
    Max_level = 0;
    key_num = 0;
    max_key = INT_MIN;
    min_key = INT_MAX;
    Size = 10272; //Header + BloomFilter
}

/*
 * @brief: insert a node into the skiplist,update the Size
 * @param key: the key of the node
 * @param s: the string of the node
 * @return: true :success
 *         false: skipList is full
 * //TODO: if delete twice?
 * */
bool SkipList::insert(uint64_t key, const string &s) {
    int curr_level = Max_level;
    node* next = head;
    node** update_list = new node*[Max_level + 1];
    if( key > max_key) max_key = key;
    if( key < min_key) min_key = key;

    while(curr_level >= 0) {
        if( key < next->point_list[curr_level]->key) {
            update_list[curr_level] = next;//后续可能需要更新
            curr_level--;
            continue;
        }
        if( key == next->point_list[curr_level]->key){
            delete []update_list;
            string old = next->point_list[curr_level]->s;
            if(Size + s.size() - old.size() > CAPACITY)
                return false;
            next->point_list[curr_level]->s = s;
            Size += s.size() - old.size();
            return true;
        }
        if( key > next->point_list[curr_level]->key)
            next = next->point_list[curr_level];
    }

    if(Size + s.size() + 13 > CAPACITY) {
        delete []update_list;
        return false;
    }
    node* NewNode = new node(key, s,next->point_list[0]);
    key_num ++;
    Size += s.size() + 13; // 1 + 8 + 4: '\0' + key + offset
    next->point_list[0] = NewNode;

//    srand(time(nullptr));
    int r = rand() % 1000;
    while(r % 1000 < P){
        r = rand() % 1000;
        NewNode->level++;
        if(NewNode->level > Max_level) {
            Max_level++;
            head->point_list.push_back(NewNode);
            NewNode->point_list.push_back(tail);
            head->level++;
            tail->level++;
            break;
        }
        else {
            NewNode->point_list.push_back( update_list[NewNode->level] -> point_list[NewNode->level] );
            update_list[NewNode->level] -> point_list[NewNode->level] = NewNode;
        }
    }
    delete []update_list;
    return true;
}

std::string SkipList::search(uint64_t key) {
    int curr_level = Max_level;
    node* next = head;

    while(curr_level >= 0) {
        if( key == next->point_list[curr_level]->key)
            return next->point_list[curr_level]->s;

        if( key < next->point_list[curr_level]->key)
            curr_level--;
        else
            next = next->point_list[curr_level];
    }

    return "";
}

bool SkipList::scan(uint64_t key1, uint64_t key2, list<std::pair<uint64_t, std::string>> &list) {
    for(int i = key1; i <= key2; ++i) {
        string tmp = search(i);
        if( ! tmp.empty())
            list.emplace_back(std::make_pair(i, tmp));
    }
}

bool SkipList::del(uint64_t key) {
    return false;
}

//void SkipList::store(const string &path, const uint64_t timestamp) {
//    ofstream out(path, ios::binary | ios::app);
//
//    // Header
//    out.write((char*)&timestamp, sizeof(uint64_t));
//    out.write((char*)&key_num, sizeof(uint64_t));
//    out.write((char*)&min_key, sizeof(uint64_t));
//    out.write((char*)&max_key, sizeof(uint64_t));
//
//    // Bloom Filter
//    uint32_t hash[4] = {0};
//    node *curr = head->point_list[0];
//    while(curr != tail) {
//        MurmurHash3_x64_128(&curr->key, sizeof(uint64_t), 1, hash);
//        for(int i = 0; i < 4; ++i)
//            bits.set(hash[i] % 81920);
//        curr = curr->point_list[0];
//    }
//    out.write((char*)&bits, sizeof(bits));
//
//    // key + offset
//    uint32_t offset = 10272 + key_num * 12; //begin of value area, 12 = key + offset = 8 + 4
//    curr = head->point_list[0];
//    while(curr != tail) {
//        out.write((char*)&curr->key, sizeof(uint64_t));
//        out.write((char*)&offset, sizeof(uint32_t));
//        offset += curr->s.size() + 1;
//        curr = curr->point_list[0];
//    }
//
//    // Data area
//    curr = head->point_list[0];
//    while(curr != tail) {
//        out.write(curr->s.c_str(), curr->s.size());
//        out.write("\0", 1);
//        curr = curr->point_list[0];
//    }
//    out.close();
//}

