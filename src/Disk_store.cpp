#include "../header/Disk_store.h"

DiskStore::DiskStore() {
    level_num = 0;
}

void DiskStore::add_level(DiskLevel::LEVEL_MODE mode) {
    uint64_t max_num = level_list.end()->max_num * 2;
    level_list.emplace_back(DiskLevel(max_num, mode));
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
uint64_t DiskStore::add_sstable(SSTable &sstable, uint32_t level) {
    if (level > level_num) {
        return -2;
    }
    DiskLevel curr_level = level_list[level];
    uint64_t serial = curr_level.add_sstable(sstable);

    if(curr_level.max_num <= curr_level.sstable_num)
        return -1;

    return serial;
}

std::string DiskStore::get(const uint64_t key) const{
    string res = "";
    uint64_t timestamp = -1;
    for(int i = 0; i < level_num; i++) {
        for(SSTable sstable: level_list[i].sstable_list) {
            if(sstable.get_min_key() > key || sstable.get_max_key() < key
                    || sstable.get_time_stamp() <= timestamp)
                continue;
            //在文件中找，需要DiskStore寻找，sstable不知道自己在哪一层
            uint32_t offset,len;
            if( sstable.get(key,offset,len)) {
                timestamp = sstable.get_time_stamp();
                //读取文件
                string directory = string(DATA_PATH )+string(FILE_PREFIX) + to_string(i);
                string file = directory + '/' +  to_string(timestamp) + '-' + to_string(sstable.get_serial()) + ".sst";
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
