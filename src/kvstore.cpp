#include "../header/kvstore.h"
#include "../header/utils.h"
#include <string>
#include <sys/stat.h>
string DATA_PATH = "";
string FILE_PREFIX = "";
KVStore::KVStore(const std::string &dir ): KVStoreAPI(dir)
{
    memtable = new SkipList();
    disk_store = new DiskStore((string)CONFIG_DIR);
    timestamp = 1;
    DATA_PATH = dir + "/";
    FILE_PREFIX = dir + "/level-";
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
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string res="";
    if( !memtable)
        memtable = new SkipList();
    else
        res = memtable->search(key);
    // 被删除
    if(res == (string)DELETE_VAL)
        return "";

    //不在memtable中
    if(res.empty())
        res = disk_store->get(key);
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
    if (memtable->del(key))
        return true;
    // 说明memtable中没有
    if(disk_store->get_level_num() == 0) // 没有磁盘文件
        return false;
    // 去磁盘中找
    if(get(key).empty())
        return false;

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
        string dir_path = DATA_PATH + dir;
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
    disk_store = new DiskStore();
    timestamp = 1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    memtable->scan(key1, key2, list);
}

void KVStore::write_to_L0() {
//    string directory = string(DATA_PATH )+string(FILE_PREFIX) + "0";
//    string filename = directory + '/' + to_string(timestamp) + ".sst";
//    level_size[0] ++;
//    timestamp++;
//
//    struct stat st{};
//    int rc = stat(directory.c_str(), &st); // 使用 c_str() 将 string 转换为字符数组类型char*
//    if ( !(rc == 0 && S_ISDIR(st.st_mode))) {
//        // 目录不存在
////        mkdir(directory.c_str());
//    }
//
//    ss_table->store(filename, timestamp);

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
            uint64_t time_stamp, serial;
            sscanf(file.c_str(), "%lu-%lu.sst",&time_stamp, &serial);
            SSTable* ssTable = new SSTable(file_path, time_stamp, serial);
            disk_store->add_sstable(ssTable, level);
        }
    }
}

/*
 * file_name: timestamp-serial.sst
 * */
void KVStore::dump() {
    string directory = string(DATA_PATH )+string(FILE_PREFIX) + "0";
    if (!utils::dirExists(directory)) {
        utils::mkdir(directory.c_str());
        if( (disk_store->get_level_num() == 0)) {
            // 层未创建过,level0 始终是tiering
            disk_store->add_level(DiskLevel::TIERING);
        }
    }
    SSTable *ss_table = new SSTable(memtable, timestamp);
    uint64_t serial =  disk_store->add_sstable(ss_table, 0);
    if(serial >= 0) {
        // 保存成文件
        string file = directory + '/' +  to_string(timestamp) + '-' + to_string(serial) + ".sst";
        ss_table->save_file(file);
    }
    timestamp++;

    // 清空memtable
    delete memtable;
    memtable = new SkipList();
}