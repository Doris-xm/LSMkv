#include "../header/SSTable.h"

//SSTable

SSTable::SSTable(SkipList *skip_list, uint64_t &time_stamp) {
    node* skip_header = skip_list->get_head();
    time_stamp = TIME_STAMP++;
    // create buff_table_global_info
    bloom_filter = new BloomFilter();
    header = new Header(time_stamp,
                        skip_list->get_key_num(),
                        skip_list->get_max_key(),
                        skip_list->get_min_key());
    uint32_t offset = 0; //TODO:offset start from?
    node* curr = skip_list->get_head();
    node* tail = skip_list->get_tail();
    while (curr != tail) {
        Indexer indexer(curr->key, offset);
        index_area.emplace_back(indexer);
        bloom_filter->insert(curr->key);
        data_area.emplace_back(curr->s);
        offset += curr->s.size();
        curr = curr->point_list[0];
    }
}

SSTable::~SSTable() {
    delete header;
    delete bloom_filter;
};
SSTable::SSTable() {
    header = nullptr;
    bloom_filter = nullptr;
}

