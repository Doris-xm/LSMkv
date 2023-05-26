# README

主要结构：

* `memTable` ：跳表
* `Disk_store` 
  * `Disk_Level` ：存储行的基本属性，以及`SSTable` 的vector
  * `SSTable` ：硬盘存储单元
    * `Header`
    * `BloomFilter`
    * `index_area`
    * `data_area` ：只是暂存，写入文件后内存被释放



TODO:

二进制读写有问题，读到乱码

compaction测试未覆盖

