#include <iostream>
#include "bitset"
#include "fstream"
#define BLOOM_SIZE 81920
using namespace std;
int main() {
    string file_path = "./data/level-0/11-0.sst";
    ofstream out(file_path, ios::binary | ios::out);
    if (!out.is_open()) {
        cout << "open file error: "<<file_path << endl;
        return -1;
    }

    // write header
    for(uint64_t i=0;i<4;i++){
        out.write((char*)(&i), 8);
    }

    // write bloom_filter
    std::bitset<BLOOM_SIZE> zero;
    out.write((char *)(&zero), sizeof(zero));

    // write index_area
    for(uint64_t i=0;i<2029;i++){
        uint32_t j = i;
        out.write((char*)(&i), 8);
        out.write((char*)(&j), 4);
    }

    // write data_area
    for (uint64_t i = 0; i < 2029; ++i) {
        std::string s(i+1, 's');
        out.write(s.c_str(), (long long)s.size());
    }

    out.close();
//    for (const auto &item : data_zone.content_) {
//        f.write(item.c_str(), (long long) item.size());
//        // example: char* res = new char[size], f.read(res, size)
//    }

    ifstream in(file_path, ios::binary | ios::in);
    if (!in.is_open()) {
        cout << "open file error: "<<file_path << endl;
        return -1;
    }
    // read header
    uint64_t time_stamp, serial,total_num,min_key,max_key;
    in.read((char*)&time_stamp, 8);
    in.read((char*)&total_num, 8);
    in.read((char*)&min_key, 8);
    in.read((char*)&max_key, 8);
    cout<<time_stamp<<" "<<total_num<<" "<<min_key<<" "<<max_key<<endl;

    // read bloom_filter
    bitset<BLOOM_SIZE> filter;
    in.read((char *)(&filter), sizeof(filter));
//    for(int i=0;i<BLOOM_SIZE;i++){
//        cout<<filter[i];
//    }
//    cout<<endl;

    // read index_area
    uint64_t key;
    uint32_t offset;
    for (int i = 0; i < 2029; ++i) {
        in.read((char*)&key, 8);
        in.read((char*)&offset, 4);
        cout<<key<<" "<<offset<<endl;
    }
    streampos curr = in.tellg(); //获取当前位置
    in.seekg(34620, ios::beg); //定位到文件的第offset个字节
    int len = 1;
    streampos start = in.tellg(); //获取当前位置
    if (len < 0) {
        in.seekg(0, ios::end); //定位到文件末尾
        std::streampos end = in.tellg(); //获取当前位置
        len = end - start;
        in.seekg(offset, ios::beg); //定位回到文件的第offset个字节,准备读
    }

//    char *tmp = new char[len + 1];
//    in.read(tmp, len);
//    tmp[len] = '\0';
//    string res = tmp;
//    delete[]tmp;

//    char *res = new char[len + 1];
//    in.read(res, len);
//    res[len] = '\0';
//    std::string ret = res;
//    in.close();
//    delete[]res;
    string res(len+1, ' ');
    in.read(&(*res.begin()), sizeof(char) * len);
    res[len] = '\0';
    in.close();
    cout<<res<<endl;
    return 0;
}
