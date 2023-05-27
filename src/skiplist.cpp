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
    tail = new node(-1);//origin used INT_MAX
    head = new node(0,"",tail);
    Max_level = 0;
    key_num = 0;
    max_key = 0;
    min_key = -1;
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


    while(curr_level >= 0) {
        if( key < next->point_list[curr_level]->key) {
            update_list[curr_level] = next;//后续可能需要更新
            curr_level--;
            continue;
        }
        if( key == next->point_list[curr_level]->key){
            delete []update_list;
            string old = next->point_list[curr_level]->s;
            if(Size + s.size() - old.size() > CAPACITY) //替换旧值时超出容量，不替换，dump后插入新的memtable
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
    if( key > max_key) max_key = key;
    if( key < min_key) min_key = key;
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
    if(key1 > max_key || key2 < min_key) return false;
    for(int i = key1; i <= key2; ++i) {
        string tmp = search(i);
        if( ! tmp.empty())
            list.emplace_back(std::make_pair(i, tmp));
    }
    return true;
}


//int SkipList::del(uint64_t key) {
//    return false;
//}

