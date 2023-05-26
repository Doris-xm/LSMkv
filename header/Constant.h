#pragma once
#include <cstdio>
#include <vector>
#include <fstream>
//#include "bitset"
#include <string>
#include <climits>
#include <list>

#define DELETE_VAL "~DELETED~"
#define CONFIG_DIR "../config/config.txt"
#define CAPACITY 2097152

const uint32_t BLOOM_SIZE = 81920;
const uint64_t HEADER_BYTE_SIZE = 10272;
const uint64_t TWO_MB = 2097152;