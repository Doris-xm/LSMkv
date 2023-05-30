#include "../header/SSTable.h"
#include "iostream"
//SSTable

SSTable::SSTable(SkipList *skip_list, const uint64_t time_stamp) {
    // create buff_table_global_info
    bloom_filter = new BloomFilter();
    header = new Header(time_stamp,
                        skip_list->get_key_num(),
                        skip_list->get_max_key(),
                        skip_list->get_min_key());
    uint32_t offset = 32 + 10240 + header->total_num * 12; // 32: header size, 10240: bloom_filter size, 12 = 8+4: index_area size
    node* curr = skip_list->get_head()->point_list[0];
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

SSTable::SSTable(const vector<pair<uint64_t,string>>& data, const uint64_t time_stamp) {
    bloom_filter = new BloomFilter();
    uint64_t key_num = data.size(), max_key = data[key_num - 1].first, min_key = data[0].first;
    header = new Header(time_stamp, key_num, max_key, min_key);
    uint32_t offset = 32 + 10240 + header->total_num * 12; // 32: header size, 10240: bloom_filter size, 12 = 8+4: index_area size
    for (auto iter : data) {
        Indexer indexer(iter.first, offset);
//        if(iter.first == 3316)
//            cout << "debug" << endl;
        index_area.emplace_back(indexer);
        bloom_filter->insert(iter.first);
        data_area.emplace_back(iter.second);
        offset += iter.second.size();
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
    ofstream out(file_name, ios::binary | ios::out);
    if (!out.is_open()) {
        cout << "open file error: "<<file_name << endl;
        return;
    }

    // write header
    out.write((char*)(&header->time_stamp), 8);
    out.write((char*)(&header->total_num), 8);
    out.write((char*)(&header->min_key), 8);
    out.write((char*)(&header->max_key), 8);

    // write bloom_filter
    std::bitset<BLOOM_SIZE> filter;
    bloom_filter->getBit(filter);
    out.write((char *)(&filter), sizeof(filter));

    // write index_area
    for(auto iter : index_area) {
        out.write((char*)&iter.key, 8);
//        if(iter.key == 3316)
//            cout<<"debug"<<endl;
        out.write((char*)&iter.offset, 4);
    }
    // write data_area
    for (auto iter : data_area) {
//        if(iter.size() == 3317)
//            cout<<"debug"<<endl;
        out.write(iter.c_str(), (long long)iter.size());
    }
    out.close();

    vector<string>().swap(data_area); //释放data_area的内存
}

bool SSTable::get(const uint64_t key, uint32_t & offset, int & size) {
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

SSTable::SSTable(const string &file_path, uint64_t time_stamp,uint64_t serial) {
    ifstream in(file_path, ios::binary | ios::in);
    if (!in.is_open()) {
        cout << "open file error: "<<file_path << endl;
        return;
    }
    // read header
    header = new Header();
    in.read((char*)&header->time_stamp, 8);
    in.read((char*)&header->total_num, 8);
    in.read((char*)&header->min_key, 8);
    in.read((char*)&header->max_key, 8);

    // read bloom_filter
    bitset<BLOOM_SIZE> filter;
    in.read((char *)(&filter), sizeof(filter));
    bloom_filter = new BloomFilter(filter);

    // read index_area
    for (int i = 0; i < header->total_num; ++i) {
        Indexer indexer;
        in.read((char*)&indexer.key, 8);
        in.read((char*)&indexer.offset, 4);
        index_area.emplace_back(indexer);
    }

    in.close();
    header->time_stamp = time_stamp;
    Serial = serial;
}
/*
 * @brief:将SSTable中的数据区读取到内存中
 * @param:file_path:文件路径
 * @param:返回一个包含键值对的vector:data
 * @param:is_end:是否是最后一行，最后一行需要删除所有的~DELETE~标记
 * */
void SSTable::read_to_mem(const string &file_path, vector< pair <pair<uint64_t, string>,uint64_t> >  &data) {
    ifstream in(file_path, ios::binary | ios::in);
     if (!in.is_open()) {
        cout << "SSTable::read_to_mem::open file error: "<<file_path << endl;
        return;
    }
    in.seekg(index_area[0].offset, ios::beg);
    uint32_t len;

    for (int i = 0; i < header->total_num - 1; ++i) {
        len = index_area[i + 1].offset - index_area[i].offset;
        string ans(len, ' ');
        in.read(&(*ans.begin()), sizeof(char) * len);
        ans[len] = '\0';
//        if(!is_end || strcasecmp(ans.c_str(), "~DELETE~") != 0)
            data.emplace_back(make_pair(make_pair(index_area[i].key, ans), header->time_stamp));
    }
    streampos start = in.tellg(); //获取当前位置
    in.seekg(0, ios::end); //定位到文件末尾
    streampos end = in.tellg(); //获取当前位置
    in.seekg(start); // 重新定位文件指针到起始位置
    len = end - start;
    string ans(len, ' ');
    in.read(&(*ans.begin()), sizeof(char) * len);
    ans[len] = '\0';
//    if(!is_end || strcasecmp(ans.c_str(), "~DELETE~") != 0)
        data.emplace_back(make_pair(make_pair(index_area[header->total_num - 1].key, ans), header->time_stamp));
}




