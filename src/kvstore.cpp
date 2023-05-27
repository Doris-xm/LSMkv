#include "../header/kvstore.h"
#include "../utils.h"
#include <string>
#include "iostream"
#include <sys/stat.h>
string DATA_PATH = "";
string FILE_PREFIX = "";
KVStore::KVStore(const std::string &dir ): KVStoreAPI(dir)
{
    memtable = new SkipList();
    disk_store = new DiskStore((string)CONFIG_DIR);
    timestamp = 1;
    DATA_PATH = dir;
    FILE_PREFIX = "/level-";
    init();
}

KVStore::~KVStore()
{
    if(memtable ) {
        dump();
    }
    delete memtable;
    delete disk_store;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(memtable == nullptr)
        memtable = new SkipList();
    if(!memtable->insert(key, s)) {
        dump();
        memtable->insert(key, s);
    }

}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string res="";
    if( !memtable) // 这种情况只可能是其他函数delete后忘记new
        memtable = new SkipList();
    else
        res = memtable->search(key);
    // 被删除
    if(res == (string)DELETE_VAL)
        return "";

    //不在memtable中
    if(res.empty()) {
        string directory = string(DATA_PATH )+string(FILE_PREFIX);
        res = disk_store->get(key,directory);
    }
    if(res == (string)DELETE_VAL)
        return "";
    return res;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    // 先去memtable删除
    string res = memtable->search(key);
    if (res == (string)DELETE_VAL)// 重复删除
        return false;
    if ( res.empty()) {
        // 说明memtable中没有
        if(disk_store->get_level_num() == 0) // 没有磁盘文件
            return false;
        // 去磁盘中找
        if(get(key).empty())
            return false;
    }
    put(key, (string)DELETE_VAL);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    vector<string> directories;
    utils::scanDir(DATA_PATH, directories);
    for (auto &dir : directories) {
        string dir_path = DATA_PATH +'/'+ dir;
        vector<string> files;
        utils::scanDir(dir_path, files);
        for (auto &file : files) {
            string file_path = dir_path + "/" + file;
            utils::rmfile(file_path.c_str());
        }
        utils::rmdir(dir_path.c_str());
    }

    delete memtable;
    delete disk_store;
    memtable = new SkipList();
    disk_store = new DiskStore((string)CONFIG_DIR);
    timestamp = 1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    // 先去memtable中找
    memtable->scan(key1, key2, list);
    //TODO: 去磁盘中找
}


/*
 * @brief:扫描目录，将文件加载到内存中
 * */
void KVStore::init() {
    vector<string> directories;
    utils::scanDir(DATA_PATH, directories);
    for (auto &dir : directories) {
        string dir_path = DATA_PATH + dir;
        vector<string> files;
        utils::scanDir(dir_path, files);
        for (auto &file : files) {
            string file_path = dir_path + "/" + file;
            uint32_t level;
            sscanf(dir.c_str(), "level-%u", &level);
            uint64_t time_stamp;
            uint64_t serial;
            sscanf(file.c_str(), "%lu-%lu.sst",&time_stamp, &serial);
            SSTable* ssTable = new SSTable(file_path, time_stamp, serial);
            bool flag = false;
            disk_store->add_sstable(ssTable, level,flag);
            //TODO:未检查合并（默认文件无损坏）
        }
    }
}

/*
 * file_name: timestamp-serial.sst
 * */
void KVStore::dump() {
    string directory = DATA_PATH + FILE_PREFIX + "0";
    if (!utils::dirExists(directory)) {
        utils::mkdir(directory.c_str());
        if( (disk_store->get_level_num() == 0)) {
            // 层未创建过,level0 始终是tiering
            disk_store->add_level(DiskLevel::TIERING);
        }
    }
    SSTable *ss_table = new SSTable(memtable, timestamp);
    bool compaction_flag = false;
    uint64_t serial =  disk_store->add_sstable(ss_table, 0,compaction_flag);

    // 保存成文件
    string file = directory + '/' +  to_string(timestamp) + '-' + to_string(serial) + ".sst";
    ss_table->save_file(file);
    if(compaction_flag) {
        compaction(1);
    }
    timestamp++;

    // 清空memtable
    delete memtable;
    memtable = new SkipList();
}

/*
 *
 * @param dump_to_level: dump的目标层
 * */
void KVStore::compaction(uint32_t dump_to_level) {
    if(dump_to_level <= 0 || dump_to_level > disk_store->get_level_num()) { // 上层函数错误传参情况
        return;
    }

    // 创建目录前缀，让disk解决compaction
    string directory = string(DATA_PATH )+string(FILE_PREFIX);
    disk_store->compaction(dump_to_level, directory);

}
