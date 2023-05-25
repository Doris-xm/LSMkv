#include "../header/Disk_store.h"

DiskStore::DiskStore() {
    level_num = 0;
}

void DiskStore::add_level(DiskLevel::LEVEL_MODE mode) {
    uint64_t max_num = level_list.end()->max_num * 2;
    level_list.emplace_back(DiskLevel(max_num, mode));
    level_num++;
}
