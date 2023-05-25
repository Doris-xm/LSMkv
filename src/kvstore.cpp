#include "../header/kvstore.h"
#include "../header/utils.h"
#include <string>
#include <sys/stat.h>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memtable = new SkipList();
    sstable = new SkipList();
    disk_store = new DiskStore();
    memtable->Size = 10272; // Header:32 , Bloom filter:10240
    level_size.push_back(0);
    level_num = 0;
    timestamp = 0;
}

KVStore::~KVStore()
{
    delete memtable;
    delete sstable;
    delete disk_store;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    //insert & calculate size
    string old_value = memtable->insert(key, s);
    if(old_value.empty())
        memtable->Size += s.size() + 13; // 1 + 8 + 4: '\0' + key + offset
    else
        memtable->Size += s.size() - old_value.size();

    if(memtable->Size > MAX_SIZE) {
        //write memtable to sstable
        sstable = memtable;
        memtable = new SkipList();
        memtable->Size = 10272;
        write_to_L0();
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string res = memtable->search(key);
    if(res == "~DELETED~")
        return "";
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
    memtable->insert(key,"~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete memtable;
    delete sstable;
    memtable = new SkipList();
    sstable = new SkipList();
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
    string directory = string(DATA_PATH )+string(FILE_PREFIX) + "0";
    string filename = directory + '/' + to_string(timestamp) + ".sst";
    level_size[0] ++;
    timestamp++;

    struct stat st{};
    int rc = stat(directory.c_str(), &st); // 使用 c_str() 将 string 转换为字符数组类型char*
    if ( !(rc == 0 && S_ISDIR(st.st_mode))) {
        // 目录不存在
//        mkdir(directory.c_str());
    }

    sstable->store(filename, timestamp);

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
    uint64_t time_stamp = 0;
    SSTable ss_table(memtable, time_stamp); // 这个函数会设置时间戳
    // 保存成文件
//    string file = "0"
//    dump_info(file_name, ss_table.buff_table_, ss_table.data_zone_);
//    all_buffs[0][{time_stamp, TAG++}] = ss_table.buff_table_;
//    // delete skip list
//    delete list_;
//    list_ = nullptr;
}