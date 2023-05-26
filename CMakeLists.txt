cmake_minimum_required(VERSION 3.21)
project(LSMkv)

set(CMAKE_CXX_STANDARD 14)

add_executable(LSMkv_persist test/persistence.cpp header/kvstore.h header/kvstore_api.h header/MurmurHash3.h header/utils.h test/test.h src/kvstore.cpp src/skiplist.cpp header/skiplist.h header/SStable.h src/SStable.cpp header/Disk_store.h src/Disk_store.cpp header/Constant.h)

add_executable(LSMkv_correct test/correctness.cpp header/kvstore.h header/kvstore_api.h header/MurmurHash3.h header/utils.h test/test.h src/kvstore.cpp src/skiplist.cpp header/skiplist.h header/SStable.h src/SStable.cpp header/Disk_store.h src/Disk_store.cpp header/Constant.h)

add_executable(LSMkv_main main.cpp header/kvstore.h header/kvstore_api.h header/MurmurHash3.h header/utils.h test/test.h src/kvstore.cpp src/skiplist.cpp header/skiplist.h header/SStable.h src/SStable.cpp header/Disk_store.h src/Disk_store.cpp header/Constant.h)
