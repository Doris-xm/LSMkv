#include "../header/SSTable.h"
#include "iostream"
//SSTable

SSTable::SSTable(SkipList *skip_list, const uint64_t time_stamp) {
    node* skip_header = skip_list->get_head();
    // create buff_table_global_info
    bloom_filter = new BloomFilter();
    header = new Header(time_stamp,
                        skip_list->get_key_num(),
                        skip_list->get_max_key(),
                        skip_list->get_min_key());
    uint32_t offset = 32 + 10240 + header->total_num * 12; // 32: header size, 10240: bloom_filter size, 12 = 8+4: index_area size
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

void SSTable::save_file(const string &file_name) {
    ofstream out(file_name, ios_base::binary | ios_base::out);
    if (!out.is_open()) {
        cout << "open file error: "<<file_name << endl;
        return;
    }
    // write header
    out.write((char*)header->time_stamp, 8);
    out.write((char*)header->total_num, 8);
    out.write((char*)header->min_key, 8);
    out.write((char*)header->max_key, 8);

    // write bloom_filter
    std::bitset<BLOOM_SIZE> filter;
    bloom_filter->getBit(filter);
    out.write((char *)(&filter), sizeof(filter));

    // write index_area
    for(auto iter : index_area) {
        out.write((char*)&iter.key, 8);
        out.write((char*)&iter.offset, 4);
    }

    // write data_area
    for (auto iter : data_area) {
        out.write((char*)&iter, iter.size());
    }
    out.close();

    vector<string>().swap(data_area); //释放data_area的内存
}

bool SSTable::get(const uint64_t key, uint32_t & offset, uint32_t & size) {
    // 现在BloomFilter中查找,如果没有，就一定没有
    if ( ! bloom_filter->find(key))
        return false;

    // 在index_area中二分查找，可能没有
    int index = binary_search(key);
    if(index < 0)
        return false;

    // 在data_area文件中查找
    offset = index_area[index].offset;
    size = -1;
    if((index + 1) < index_area.size()) // 如果是最后一个元素，len=-1
        size = index_area[index + 1].offset - offset;

    return true;
}

int SSTable::binary_search(const uint64_t key) const {
    int left = 0;
    int right = index_area.size() - 1;

    while (left <= right) {
        int mid = left + (right - left) / 2;

        if (index_area[mid].key == key) {
            return mid; // 找到了指定的 key 值，返回下标
        } else if (index_area[mid].key < key) {
            left = mid + 1; // 在右半部分继续查找
        } else {
            right = mid - 1; // 在左半部分继续查找
        }
    }

    return -1; // 没有找到指定的 key 值
}



