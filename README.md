# README

#### 主要结构

* `memTable` ：跳表
* `Disk_store` ：磁盘存储部分
  * `Disk_Level` ：存储行的基本属性，以及`SSTable` 的vector
  * `SSTable` ：硬盘存储单元（只存索引部分）
    * `Header`
    * `BloomFilter`
    * `index_area`
    * `data_area` ：只是暂存，写入文件后内存被释放（其实该结构不需要写入结构体）

#### 文件目录

```
.
├── Makefile  
├── CMakeLists.txt
├── README.md 
├── data      // 数据存储路径
├── config   // disk层数配置文件路径
├── utils.h        
├── main.cpp  // debug用
├—— test	//测试用
	├── correctness.cpp
	├── persistence.cpp
	├──test.h
├── src
	├──Disk_store.cpp
	├──kvstore.cpp
	├──skiplist.cpp
	├──SStable.cpp
├── header
	├──Constant.h
	├──Disk_store.h
	├──kvstore.h
	├──kvstore_api.h
	├──MurmurHash3.h
	├──skiplist.h
	├──SStable.h

```





#### 主要接口注解

##### 1. `SSTable`

1. `SSTable(SkipList *skip_list, uint64_t time_stamp)`：

通过跳表构造SSTable，用于`dump()`操作，需指定时间戳

2. `SSTable(const vector<pair<uint64_t,string>>& data, const uint64_t time_stamp)`：

   通过存放键值对的vector构造SStable，用于`Compaction`操作中的重构sstable，需指定时间戳。

3. `SSTable(const string &file_path, uint64_t time_stamp,uint64_t serial)`

   通过读取文件构建SStable，用于init操作，其中时间戳和序列号是通过文件名解析得到（为了方便，直接上级函数解析完成后传入）

4. `bool get(uint64_t key, uint32_t & offset, int & size)`：

5. `void save_file(const string &file_name)`

   将SStable中的部分写入，用于`dump`操作，需给定文件名

6. `void read_to_mem(const string &file_path, vector< pair <pair<uint64_t, string>,uint64_t> >  &data)`

   从内存中读取键值对，需给定文件名。在vector中对每个键值对做了时间标记（TODO：优化？），用于`Compaction`中的合并。



##### 2. `Disk_Level`

1. `uint64_t add_sstable(SSTable* sstable)`

   给当前`level` 增加一个sstable，`Disk_Level`需要维护每次的sstable数量，以及一个自增的序列号`serial`

2. `void choose_sstables(vector<SSTable*> &sstable_list, uint64_t min_key, uint64_t max_key,int mode)`

   `Disk_Level`按规则选择sstable，用于`Compaction`，返回的sstable列表。

   @param: mode  : 1:全部选择 2:按时间选择多余的 3:选择key有重叠的

##### 3. `Disk_store`

1. `string get(const uint64_t key,const string & dir_prefix)const`

   从磁盘中找，遍历每层。

2. `void compaction(uint32_t dump_to_level, const string & dir)`

   递归compaciton。

   @param  `dump_to_level` 目标层

   @param `dir` ：文件前缀（路径）



#### 4. `kvstore`

1. `void put(uint64_t key, const std::string &s) override`

   插入键值对，首先插入memtable，并判断是否需要dump（以及compaction）。

2. `string get(uint64_t key) override`

   查询键值对，首先从memtable内存寻找，如果没有，甩给disk。

3. `bool del(uint64_t key) override`

   删除键值对

4. void reset() override

   重置：删除所有文件，计时器time_stamp归零。

5. `void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override`

   TODO：只实现了memtable中的scan