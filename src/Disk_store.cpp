#include "../header/Disk_store.h"
#include <iostream>

DiskStore::DiskStore(const string &config_dir) {
    ifstream in(config_dir, std::ios::in);
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
            if(strcasecmp(mode.c_str(),"Tiering") == 0)
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
 * @param: level: the level to be added
 * @return: serial > 0: success sstable的序列号
 *         -1: need to dump to next level
 *         -2: level is out of range
 * */
uint64_t DiskStore::add_sstable(SSTable* sstable, uint32_t level) {
    if (level > level_num) {
        return -2;
    }
    DiskLevel* curr_level = level_list[level];
    uint64_t serial = curr_level->add_sstable(sstable);

    if(curr_level->max_num <= curr_level->sstable_num)
        return -1;

    return serial;
}

std::string DiskStore::get(const uint64_t key) const{
    string res = "";
    uint64_t timestamp = -1;
    for(int i = 0; i < level_num; i++) {
        for(SSTable* sstable: level_list[i]->sstable_list) {
            if(sstable->get_min_key() > key || sstable->get_max_key() < key
                    || sstable->get_time_stamp() <= timestamp)
                continue;
            //在文件中找，需要DiskStore寻找，sstable不知道自己在哪一层
            uint32_t offset,len;
            if( sstable->get(key,offset,len)) {
                timestamp = sstable->get_time_stamp();
                //读取文件
                string directory = string(DATA_PATH )+string(FILE_PREFIX) + to_string(i);
                string file = directory + '/' +  to_string(timestamp) + '-' + to_string(sstable->get_serial()) + ".sst";
                res = read_file(file,offset,len);
            }
        }
    }
    return res;
}

string DiskStore::read_file(const string &file_name, uint32_t offset, uint32_t len) const {
    fstream in;
    in.open(file_name, std::ios_base::binary | std::ios_base::in);
    in.seekg(offset, std::ios::beg); //定位到文件的第offset个字节
    std::streampos start = in.tellg(); //获取当前位置
    if (len < 0) {
        in.seekg(0, std::ios::end); //定位到文件末尾
        std::streampos end = in.tellg(); //获取当前位置
        len = end - start;
        in.seekg(offset, std::ios::beg); //定位回到文件的第offset个字节,准备读
    }

    char *tmp = new char[len + 1];
    in.read(tmp, len);
    tmp[len] = '\0';
    string res = tmp;
    in.close();
    delete[]tmp;
    return res;
}
