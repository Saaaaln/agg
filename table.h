//
// Created by he on 12/4/18.
//
#ifndef AGGREGATION_TABLE_H
#define AGGREGATION_TABLE_H

#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <snappy.h>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>

#define BLOCK_SIZE 65536
#define DEBUGINFO 1
using namespace std;
struct Table {
    string name;
    string path;
    uint64_t raw_tuple_size;
    uint64_t tuple_size;
    uint64_t tuple_num;
    int upper;
    void *start;
    std::vector<int> offset, size;
} *tb[10];
//
//bool read_data_in_memory(Table *table, int repeats = 1) {
//    fstream iofile(table->path.c_str(), ios::in | ios::out | ios::binary);
//    if (!iofile) {
//        cerr << "open error!" << endl;
//        abort();
//    }
//    char start[BLOCK_SIZE];
//    void *addr = NULL;
////    cout << "blcoksize:" << BLOCK_SIZE << endl;
//    uint64_t size = table->tuple_num * table->tuple_size, tuple_num = 0;
////    cout << "size:" << size << endl;
//    addr = aligned_alloc(64, size * repeats);
//    assert(addr != NULL && "limited memory");
//    uint64_t dest_off = 0;
//    iofile.seekg(0, ios::beg);
//    while (iofile.read(start, BLOCK_SIZE)) {
//        int num = *(int *) (start + BLOCK_SIZE - 4);
////        cout << "num:" << num << endl;
//        tuple_num += num;
////        cout << "tuple_num:" << tuple_num << endl;
//        // copy needed cells from each row
//        for (int i = 0; i < num; ++i) {
////            cout << "i:" << i << endl;
//            for (int m = 0; m < table->offset.size(); ++m) {
////                cout << "m :" << m << endl;
////                cout << "dest_off1:" << dest_off << endl;
////                cout << "i * table->raw_tuple_size + table->offset[m]:"
////                     << i * table->raw_tuple_size + table->offset[m] << endl;
//                memcpy(addr + dest_off,
//                       (start + i * table->raw_tuple_size + table->offset[m]),
//                       table->size[m]);
//                dest_off += table->size[m];
//            }
//        }
//    }
////    cout << "dest_off:"<<dest_off << endl;
////    cout << "size:"<<size << endl;
//    assert(dest_off == size);
//    for (int r = 1; r < repeats; ++r) {
//        memcpy(addr + size * r, addr, size);
//    }
//    iofile.close();
//    table->start = addr;
//    table->tuple_num = table->tuple_num * repeats;
//    table->upper = table->tuple_num;  // special
//#if DEBUGINFO
//    cout << table->name << " tuple num = " << table->tuple_num << " but read "
//         << tuple_num << " repeats= " << repeats << endl;
//#endif
//    assert(table->tuple_num == tuple_num * repeats);
//    return true;
//}


void SetVectorValue(int *array, int num, vector<int> &vec) {
    for (int i = 0; i < num; ++i) {
        vec.push_back(array[i]);
    }
    return;
}

int SumOfVector(vector<int> &vec) {
    int sum = 0;
    for (int i = 0; i < vec.size(); ++i) {
        sum += vec[i];
    }
    return sum;
}

int LoadFromDisk() {
    int ret = 0;
    unsigned offset = 0;
    unsigned length = 64 * 1024 * 1024;
    cout << "CHUNK OFFSET: " << offset << endl;
    int fd = open("/home/he/imdb/tpch/T0G0P0", O_RDONLY);
    if (fd == -1) {
        cout << "open error" << endl;
    } else
        cout << "open success" << endl;

    long int file_length = lseek(fd, 0, SEEK_END);

    uint64_t start_pos = 0;
    char *tmp = (char *) malloc(64 * 1024);
    string *result = new string;
    string tmp2;
    tmp2.clear();

    stringstream str;
    char head[100];
    size_t totoal_read = 0;
//  long start_pos = CHUNK_SIZE * offset;

    cout << "start_pos=" << start_pos << "*********" << endl;
    while (totoal_read < length) {
        memset((void *) tmp, '\0', 64 * 1024);
        result->clear();

        if (start_pos < file_length) {
            lseek(fd, start_pos, SEEK_SET);
            ret = read(fd, head, 100);
            size_t compress_length = atoi(reinterpret_cast<const char *>(head));
            cout << compress_length << endl;
            start_pos += 100;
            lseek(fd, start_pos, SEEK_SET);
            ret = read(fd, tmp, compress_length);
            snappy::Uncompress(tmp, compress_length, result);
            cout << *result << endl;
            str << *result;
//      LOG(INFO) << "HOW LONG THE RESULT: " << (*result).length() << endl;
            totoal_read += BLOCK_SIZE;
            start_pos += compress_length;
//      LOG(INFO) << "Finish the one size of block" << totoal_read << endl;
        } else {
            ret = 0;
            break;
        }
    }
    string final = str.str();
//  memcpy(reinterpret_cast<void*>(desc), final.data(), length);
//    final.copy((char *) desc, length, 0);
//    chunkid_off_in_file_[next_chunk] =
//            start_pos;
    free(tmp);
//    tmp = NULL;
//    delete
//            result;
//    FileClose(fd);
//    return
//            totoal_read;
}

#endif //AGGREGATION_TABLE_H
