#include <cstring>
#include "../header/Disk_store.h"
#include <iostream>
#include <numeric>
#include <algorithm>
#include "queue"

DiskStore::DiskStore(const string &config_dir) {
    ifstream in(config_dir, ios::in);
    if(!in.is_open()) {
        cout << "open file error: "<<config_dir << endl;
        return;
    }
    string line;
    int i = 0; //检验config中的下标是否连续
    while (getline(in, line)) {
        istringstream iss(line); // 从string读取数据的流
        int index, max_size;
        string mode;
        DiskLevel::LEVEL_MODE MODE;
        if (iss >> index >> max_size >> mode) {
            if(mode == "Tiering")
//            if(strcasecmp(mode.c_str(),"Tiering") == 0) //BUG? makefile不支持strcasecmp
                MODE = DiskLevel::LEVEL_MODE::TIERING;
            else
                MODE = DiskLevel::LEVEL_MODE::LEVELING; //错误处理默认为leveling
            if(index < i) {
                config[index].first = max_size;
                config[index].second = MODE;
            }
            else if(index > i) {
                for(i; i < index; i++) {
                    config.emplace_back(make_pair(-1,DiskLevel::LEVEL_MODE::LEVELING)); //-1表示没有设置max_size，默认为leveling
                }
            }
            config.emplace_back(make_pair(max_size,MODE));
            i++;
        }
    }

    in.close();
    level_num = 0;
}
DiskStore::~DiskStore() {
    for(auto level:level_list){
        delete level;
    }
}
/*
 * @brief: 向磁盘中添加一个level
 * @param: mode: level的模式
 * @detail: 先检查config,如果config中有定义，则按照config中的定义添加
 *         如果config中没有定义，则按照默认的规则添加: max_size = 上一层的max_size * 2
 *         其中level0 必须是tiering
 * */
void DiskStore::add_level(DiskLevel::LEVEL_MODE mode) {
    uint32_t max_size;
    if(level_num == 0) { //level0 必须是tiering
        if(!config.empty() && config[0].first > 0)
            max_size = config[0].first;
        else
            max_size = 2;
        level_list.emplace_back(new DiskLevel(max_size, DiskLevel::LEVEL_MODE::TIERING));
        level_num++;
        return;
    }

    if((level_num  + 1) <= config.size()) { //config 里有定义
        if(config[level_num].first == -1) { //没有设置max_size,默认增加两倍
            level_list.emplace_back(new DiskLevel(level_list[level_num - 1]->max_num * 2,
                                                  config[level_num].second));
        }
        else
            level_list.emplace_back( new DiskLevel(config[level_num].first, config[level_num].second));
        level_num++;
        return;
    }

    //config 里无定义
    level_list.emplace_back(new DiskLevel(level_list[level_num - 1]->max_num * 2, mode));
    level_num++;
}

/*
 * @brief: add sstable to disk store
 * @param: sstable: the sstable to be added
 * @param: flag: true 需要compaction
 * @return: serial > 0: success sstable的序列号
 *         -1: level is out of range
 * */
uint64_t DiskStore::add_sstable(SSTable* sstable, uint32_t level, bool &flag) {
    if (level > level_num || level < 0 ) {
        return -1;
    }
    if(level == level_num) {
        add_level(DiskLevel::LEVEL_MODE::LEVELING);
    }
    DiskLevel* curr_level = level_list[level];
    uint64_t serial = curr_level->add_sstable(sstable);

    if(curr_level->max_num < curr_level->sstable_num)
        flag = true;

    return serial;
}

void DiskStore::read_sstable(SSTable* sstable, uint32_t level) {
//    if (level > level_num || level < 0 ) {
//        return -1;
//    }
//    if(level == level_num) {
//        add_level(DiskLevel::LEVEL_MODE::LEVELING);
//    }
    while(level_list.size() <= level)
        add_level(DiskLevel::LEVEL_MODE::LEVELING);
    DiskLevel* curr_level = level_list[level];
    curr_level->read_sstable(sstable);
}

std::string DiskStore::get(const uint64_t key,const string & dir_prefix) const{
    string res = "";
    uint64_t timestamp = 0;
    for(int i = 0; i < level_num; i++) {
        for(SSTable* sstable: level_list[i]->sstable_list) {
            if(sstable->get_min_key() > key || sstable->get_max_key() < key
                    || sstable->get_time_stamp() <= timestamp)
                continue;
            //在文件中找，需要DiskStore寻找，sstable不知道自己在哪一层
            uint32_t offset;
            int len;
            if( sstable->get(key,offset,len)) {
                timestamp = sstable->get_time_stamp();
                //读取文件
                string directory = dir_prefix + to_string(i);
                string file = directory + '/' +  to_string(timestamp) + '-' + to_string(sstable->get_serial()) + ".sst";
                res = read_file(file,offset,len);
            }
        }
    }
    return res;
}

/*
 * @brief: 从文件中读取某个数据
 * @param: file_name: 文件名
 * @param: offset: 从文件的第offset个字节开始读
 * @param: len: 读取的长度，如果为-1说明读到文件尾
 * @return: 读取的数据
 * */
string DiskStore::read_file(const string &file_name, uint32_t offset, int len) const {
    fstream in;
    in.open(file_name, ios::binary | ios::in);
    in.seekg(offset, ios::beg); //定位到文件的第offset个字节
    streampos start = in.tellg(); //获取当前位置
    if (len < 0) {
        in.seekg(0, ios::end); //定位到文件末尾
        std::streampos end = in.tellg(); //获取当前位置
        len = end - start;
        in.seekg(offset, ios::beg); //定位回到文件的第offset个字节,准备读
    }

//    char *tmp = new char[len + 1];
//    in.read(tmp, len);
//    tmp[len] = '\0';
//    string res = tmp;
//    delete[]tmp;
    string res(len, ' ');
    in.read(&(*res.begin()), sizeof(char) * len);
    res[len] = '\0';
    in.close();
    return res;
}
/*
 * @brief: 检查level是否溢出
 * @return: true: 溢出
 *         false: 未溢出（正好等于max_num）
 * */
bool DiskStore::check_level_overflow(uint32_t level)const {
    if(level >= level_num)
        return false;
    if(level_list[level]->max_num < level_list[level]->sstable_num)
        return true;
    return false;
}

void DiskStore::compaction(uint32_t dump_to_level,const string& dir_prefix) {
    // 递归终止条件：上一层文件数小于max_size，不需要dump to 下一层
    if( ! check_level_overflow(dump_to_level - 1))
        return;

    // 创建目录,检验是否是最后一行
    string directory = dir_prefix + to_string(dump_to_level);
    if(!utils::dirExists(directory)) {
        utils::mkdir(directory.c_str());
    }
    bool is_end = false;
    if(dump_to_level == level_num) {
        add_level(DiskLevel::LEVELING);
        is_end = true;
    }

    // SSTable 选取,同时记录要被删除的文件
    vector<SSTable*> last_level_chosen;
    vector<SSTable*> this_level_chosen;

    //从dump_to_level - 1层中选取
    //若 Level x 层为 Tiering，则该层所有文件被选取
    if(level_list[dump_to_level - 1]->mode == DiskLevel::TIERING)
        level_list[dump_to_level - 1]->choose_sstables(last_level_chosen,0,0,1);
    else
        level_list[dump_to_level - 1]->choose_sstables(last_level_chosen,0,0,2);

    //遍历last_level_chosen中的文件，检查最大时间戳和键值范围
//    uint64_t latest_timestamp = 0;
    uint64_t min_key = -1, max_key = -1;
    for(SSTable* sstable: last_level_chosen) {
//        if(sstable->get_time_stamp() > latest_timestamp)
//            latest_timestamp = sstable->get_time_stamp();
        if(min_key == -1 || sstable->get_min_key() < min_key)
            min_key = sstable->get_min_key();
        if(max_key == -1 || sstable->get_max_key() > max_key)
            max_key = sstable->get_max_key();
    }

    //从dump_to_level层中选取
    if(level_list[dump_to_level]->mode == DiskLevel::LEVELING) {
        level_list[dump_to_level]->choose_sstables(this_level_chosen, min_key, max_key,3);
//        for(SSTable* sstable: this_level_chosen) {
//            if(sstable->get_time_stamp() > latest_timestamp)
//                latest_timestamp = sstable->get_time_stamp();
//        }
    }

    //把选中的文件读入内存,同时记录时间戳,用于合并相同的key
    queue< vector< pair <pair<uint64_t, string>,uint64_t> > > data_all;
    for(SSTable* sstable: last_level_chosen) {
        /*
         * data: first:<key,value>
         *       second: time_stamp
         * */
        vector< pair <pair<uint64_t, string>,uint64_t> > data;
        string filename = dir_prefix + to_string(dump_to_level - 1) + '/' + to_string(sstable->get_time_stamp()) + '-' + to_string(sstable->get_serial()) + ".sst";
        sstable->read_to_mem(filename,data);
        data_all.push(data);
    }
    for(SSTable* sstable: this_level_chosen) {
        vector< pair <pair<uint64_t, string>,uint64_t> > data;
        string filename = dir_prefix + to_string(dump_to_level ) + '/' + to_string(sstable->get_time_stamp()) + '-' + to_string(sstable->get_serial()) + ".sst";
        sstable->read_to_mem(filename,data);
        data_all.push(data);
    }

    //两两归并

    while( data_all.size() > 1) {
        vector< pair <pair<uint64_t, string>,uint64_t> > data1 = data_all.front();
        data_all.pop();
        vector< pair <pair<uint64_t, string>,uint64_t> > data2 = data_all.front();
        data_all.pop();
        vector< pair <pair<uint64_t, string>,uint64_t> > date_sorted;
        mergeSort(data1, data2,date_sorted,is_end);
        data_all.push(date_sorted);
    }

    //得到一个大的有序数组data_sorted
    vector< pair <pair<uint64_t, string>,uint64_t> > date_sorted = data_all.front();
    data_all.pop();

    //创建新的SSTable
    uint64_t len_all = date_sorted.size();
    uint64_t size = 10272;
    uint64_t new_time_stamp = date_sorted[0].second;
    vector<pair<uint64_t, string> > data_dump;
    for(uint64_t i = 0; i < len_all; ++i) {
        if(size + date_sorted[i].first.second.size() + 12 > 2097152) {
            size = 10272;
            SSTable* sstable = new SSTable(data_dump, new_time_stamp);
            int64_t serial = level_list[dump_to_level]->add_sstable(sstable);
            string filename = dir_prefix + to_string(dump_to_level) + '/' + to_string(new_time_stamp) + '-' + to_string(serial) + ".sst";
            sstable->save_file(filename);
            data_dump.clear();
            new_time_stamp = date_sorted[i].second;
        }
        data_dump.push_back(date_sorted[i].first);
        new_time_stamp = (date_sorted[i].second > new_time_stamp) ? date_sorted[i].second : new_time_stamp;
        size += date_sorted[i].first.second.size() + 12;
    }
    if(!data_dump.empty()) {
        SSTable* sstable = new SSTable(data_dump, new_time_stamp);
        int64_t serial = level_list[dump_to_level]->add_sstable(sstable);
        string filename = dir_prefix + to_string(dump_to_level) + '/' + to_string(new_time_stamp) + '-' + to_string(serial) + ".sst";
        sstable->save_file(filename);
    }

    // 删除之前的文件
    for(SSTable* sstable: last_level_chosen) {
        string filename = dir_prefix + to_string(dump_to_level - 1) + '/' + to_string(sstable->get_time_stamp()) + '-' + to_string(sstable->get_serial()) + ".sst";
        utils::rmfile(filename.data());
    }
    for(SSTable* sstable: this_level_chosen) {
        string filename = dir_prefix + to_string(dump_to_level) + '/' + to_string(sstable->get_time_stamp()) + '-' + to_string(sstable->get_serial()) + ".sst";
        utils::rmfile(filename.data());
    }

    //递归检查下一层
    compaction(dump_to_level + 1,dir_prefix);
}

/*
 * @biref: 两个有序数组归并:从小到大
 * @param: data_sorted 归并后的数组
 * */
void DiskStore::mergeSort(vector< pair <pair<uint64_t, string>,uint64_t> > data1,
                          vector< pair <pair<uint64_t, string>,uint64_t> > data2,
                          vector< pair <pair<uint64_t, string>,uint64_t> > &data_sorted, bool is_end) {
    int i = 0, j = 0;
    int len1 = data1.size(), len2 = data2.size();
    while(i < len1 && j < len2) {
//        if(data1[i].first.first == 7060 || data2[j].first.first == 7060) {
//            cout << "debug" << endl;
//            if(data2[j].first.second == (string)DELETE_VAL) {
//                cout << "debug" << endl;
//            }
//        }
        if(data1[i].first.first < data2[j].first.first) {
            data_sorted.push_back(data1[i]);
            i++;
            continue;
        }
        if(data1[i].first.first == data2[j].first.first) { // key相同,比较时间戳
//            if(data1[i].first.first == 7060 ) {
//                cout << "debug" << endl;
//            }
            if(data1[i].second < data2[j].second)
                data_sorted.push_back(data2[j]);
            else
                data_sorted.push_back(data1[i]);
            j++;i++;
            continue;
        }

        data_sorted.push_back(data2[j]);
        j++;

    }
    // 将剩余的元素添加到结果向量中
    while (i < len1) {
        data_sorted.push_back(data1[i]);
        i++;
    }
    while (j < len2) {
        data_sorted.push_back(data2[j]);
        j++;
    }
    if (is_end) {
        // 使用 erase-remove 惯用法删除元素
        data_sorted.erase(std::remove_if(data_sorted.begin(), data_sorted.end(),
                                         [](const pair<pair<uint64_t, string>,uint64_t > & p) {
                                             return p.first.second == DELETE_VAL;
                                         }), data_sorted.end());
    }

}

/* FOR TEST*/
bool DiskStore::search_disk(uint64_t key, const string &file_path,uint32_t& offset) const {
        ifstream in(file_path, ios::binary | ios::in);
        if (!in.is_open()) {
            cout << "open file error: "<<file_path << endl;
            return false;
        }
        // read header
        uint64_t time_stamp,total_num,min_key,max_key;
        in.read((char*)&time_stamp, 8);
        in.read((char*)&total_num, 8);
        in.read((char*)&min_key, 8);
        in.read((char*)&max_key, 8);

        // read bloom_filter
        bitset<BLOOM_SIZE> filter;
        in.read((char *)(&filter), sizeof(filter));

        // read index_area
        for (int i = 0; i < total_num; ++i) {
            Indexer indexer;
            in.read((char*)&indexer.key, 8);
            in.read((char*)&indexer.offset, 4);
            if(indexer.key == key) {
                offset = indexer.offset;
                return true;
            }
        }

        in.close();
    return false;


}

/*
 * @brief: 每一层的结构DiskLevel按规则选择sstable
 * @param: sstable_list 返回的sstable列表
 * @param: mode  : 1:全部选择 2:按时间选择多余的 3:选择key有重叠的
 * */
void DiskLevel::choose_sstables(vector<SSTable *> &chosen_list, uint64_t min_key, uint64_t max_key,int mode) {
    if(mode == 1) { //选择全部
        for(SSTable* sstable: sstable_list) {
            chosen_list.push_back(sstable);
        }
        sstable_list.clear(); //这一行清空但是不释放内存
        sstable_num = 0;
        return;
    }
    if(mode == 2) {
        //选择时间戳最小(时间戳相等选择键最小)的多余文件
        //按照时间戳排序
        vector<uint64_t> indices(sstable_num);// 用于记录sstable_list下标的 vector
        iota(indices.begin(), indices.end(), 0); // 初始化
        // 按照时间戳从小到大排序，时间戳相等按照键从小到大排序
        sort(indices.begin(), indices.end(), [&](uint64_t i1, uint64_t i2) {
            if(sstable_list[i1]->get_time_stamp() == sstable_list[i2]->get_time_stamp())
                return sstable_list[i1]->get_total_num() < sstable_list[i2]->get_total_num();
            return sstable_list[i1]->get_time_stamp() < sstable_list[i2]->get_time_stamp();
        });

        // 选择多余的文件,indices中的前select_num个（下标）
        uint64_t select_num = sstable_num - max_num;
        for(uint64_t i = 0; i < select_num; ++i) {
            chosen_list.push_back(sstable_list[indices[i]]);
//            sstable_list.erase(sstable_list.begin() + (int)indices[i]);
            sstable_list[indices[i]]->delete_file(); // 标记为无效
//            --sstable_num;
        }
        for (auto it = sstable_list.rbegin(); it != sstable_list.rend(); ++it) {
            SSTable* sstable = *it;
            if (sstable->isToDeleted()) {
                sstable_list.erase(std::next(it).base());  // 使用逆向迭代器删除元素
                --sstable_num;
            }
        }
//        for(uint64_t i = 0; i < select_num; ++i) {
//            sstable_list.erase(sstable_list.begin() + indices[i]);
//            --sstable_num;
//        }
        return;
    }
    uint64_t origin_size = sstable_num;
    /*BUG CODE: erase change the index i*/
//    for(uint64_t i = 0; i < origin_size; ++i) {
//        SSTable* sstable = sstable_list[i];
//        if(sstable->get_min_key() > max_key || sstable->get_max_key() < min_key) //不在范围内
//            continue;
//        chosen_list.push_back(sstable);
//        sstable_list.erase(sstable_list.begin() + (int)i);
//        --sstable_num;
//    }
    /*FIX BUG: 逆向迭代器*/
    for (auto it = sstable_list.rbegin(); it != sstable_list.rend(); ++it) {
        SSTable* sstable = *it;
        if (sstable->get_min_key() > max_key || sstable->get_max_key() < min_key) {
            continue;
        }
        chosen_list.push_back(sstable);
        /*
        * 逆向迭代器不能直接使用 erase()删除，因为该函数要求传入的是正向迭代器。
        * 需要将逆向迭代器转换为正向迭代器
        * std::next(it) 返回的是 it 的下一个迭代器，即正向迭代器，
        * 通过调用 base() 函数将其转换回逆向迭代器的形式，以便在 erase() 函数中使用。
        * */
        sstable_list.erase(std::next(it).base());  // 使用逆向迭代器删除元素

        --sstable_num;
    }


}
