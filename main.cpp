#include <iostream>
#include <cstdint>
#include <string>
#include <chrono>

#include "./test/test.h"

class CorrectnessTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t BIG = 1024 * 2;
    const uint64_t LARGE_TEST_MAX = 1024 * 32; // 1024 * 64;

    void regular_test(uint64_t max)
    {
//        static int b = 1;
        uint64_t i;
//        string path = "./config/test"+ to_string(b++) +".txt";
//        fstream file(path, ios::out | ios::trunc);
        // Test multiple key-value pairs
        auto start = chrono::high_resolution_clock::now();
        for (i = 0; i < max; ++i) {
            store.put(i, std::string(i+1, 's'));
//            if(i%30==0) {
//                auto end = chrono::high_resolution_clock::now();
//                file<< chrono::duration_cast<chrono::nanoseconds>(end - start).count() / 30 << endl;
//                start = chrono::high_resolution_clock::now();
//            }
        }
        auto end = chrono::high_resolution_clock::now();
        cout<< "put time: " << chrono::duration_cast<chrono::nanoseconds>(end - start).count() << endl;
        cout<< "put num: "<<max<<endl;
        cout<< "put time per op: " << chrono::duration_cast<chrono::nanoseconds>(end - start).count() / max << endl;


//        // Test after all insertions
//        start = chrono::high_resolution_clock::now();
//        for (i = max/2; i < max+max/2; ++i)
//            store.get(i);
//        end = chrono::high_resolution_clock::now();
//        cout<< "get time: " << chrono::duration_cast<chrono::nanoseconds>(end - start).count() << endl;
//        cout<< "get num: "<<max<<endl;
//        cout<< "get time per op: " << chrono::duration_cast<chrono::nanoseconds>(end - start).count() / max << endl;
//
//
//        // Test deletions
//        start = chrono::high_resolution_clock::now();
//        for (i = 0; i < max; i++)
//            store.del(i);
//        end = chrono::high_resolution_clock::now();
//        cout<< "del time: " << chrono::duration_cast<chrono::nanoseconds>(end - start).count() << endl;
//        cout<< "del num: "<<max<<endl;
//        cout<< "del time per op: " << chrono::duration_cast<chrono::nanoseconds>(end - start).count() / max << endl;

    }

public:
    CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
    {
    }

    void start_test(void *args = NULL) override
    {
        std::cout << "KVStore Correctness Test" << std::endl;

        store.reset();

        std::cout << "[Simple Test]" << std::endl;
        regular_test(SIMPLE_TEST_MAX);

        store.reset();

        std::cout << "[Large Test]" << std::endl;
        regular_test(BIG);
        store.reset();

        std::cout << "[Simple Test]" << std::endl;
        regular_test(LARGE_TEST_MAX);
    }
};

int main(int argc, char *argv[])
{
    fstream in("./data/level-0/1-0.sst", ios::in | ios::binary);
    uint64_t time_stamp, total_num, min_key, max_key;
    in.read((char*)&time_stamp, 8);
    in.read((char*)&total_num, 8);
    in.read((char*)&min_key, 8);
    in.read((char*)&max_key, 8);
    cout<<time_stamp<<endl;
    cout<<total_num<<endl;
    cout<<min_key<<endl;
    cout<<max_key<<endl;
    return 0;
}
