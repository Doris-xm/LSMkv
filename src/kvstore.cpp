#include "../header/kvstore.h"
#include "../header/utils.h"
#include <string>
#include <sys/stat.h>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memtable = new SkipList();
    disk_store = new DiskStore();
    timestamp = 0;
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
    return res;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if(get(key).empty())
        return false;
    memtable->insert(key, (string)DELETE_VAL);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete memtable;
    memtable = new SkipList();
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
void KVStore::init() {
//    std::fstream config;
//    config.open(CONFIG_PATH, std::ios::in);
//    std::istringstream iss;
//    uint64_t level = 0, num = 0;
//    std::string info, mode;
//    MODE m;
//    while (std::getline(config, info)) {
//        iss.clear();
//        iss.str(info);
//        iss >> level >> num >> mode;
//        if (mode == "Tiering") m = TIERING;
//        else m = LEVELING;
//        config_[level] = std::make_pair(num, m);
//    }
//    config.close();
}
void KVStore::dump() {
    string directory = string(DATA_PATH )+string(FILE_PREFIX) + "0";
    if (!utils::dirExists(directory)) {
        utils::mkdir(directory.c_str());
        if( (disk_store->get_level_num() == 0)) {
            // 层未创建过,level0 始终是tiering
            disk_store->add_level(DiskLevel::TIERING);
        }
    }
    SSTable ss_table(memtable, timestamp);
    uint64_t serial =  disk_store->add_sstable(ss_table, 0);
    if(serial >= 0) {
        // 保存成文件
        string file = directory + '/' +  to_string(timestamp) + '-' + to_string(serial) + ".sst";
        ss_table.save_file(file);
    }
    timestamp++;

    // 清空memtable
    delete memtable;
    memtable = new SkipList();
}