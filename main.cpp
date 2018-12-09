#include <iostream>
#include <vector>
#include <sys/time.h>
#include <fstream>
#include "omp.h"
//#include "table.h"
#include "line_item_parser.h"
#include "proj_data.h"
#include "hash_table.h"

int main() {

//    proj_tpch1();

//    proj_tpch10();

    struct timeval t1, t2;
    int deltaT = 0;

    gettimeofday(&t1, NULL);
    std::cout << "begin" << std::endl;
    std::vector<ProjLineItem> *items = ReadAndParseProjLineItems("/home/he/Desktop/proj_lineitem_sf1.tbl");
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "--------load table into memory costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;

    //1048576 or  524288
    hash_table *ht = new hash_table(524288);

    gettimeofday(&t1, NULL);

    //build hash table
#pragma omp parallel for
    for (int i = 0; i < items->size(); ++i) {
        ht->insertItem((*items)[i]);
    }
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "--------build hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;

    //get count(*)
//    gettimeofday(&t1, NULL);
//    ht->count();
//    gettimeofday(&t2, NULL);
//    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//    std::cout << "--------travel hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;

    //get sum(QUANTITY)
    gettimeofday(&t1, NULL);
    ht->sum();
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "--------travel hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;

//    std::vector<int> ivec;
//
//#pragma omp parallel for firstprivate(ivec),lastprivate(ivec)
//    for (int i = 0; i < 15; ++i) {
//        ivec.push_back(i);
//        std::cout << "thread: " << omp_get_thread_num() << ", ivec.size()" << ivec.size() << std::endl;
//    }
//
//    std::cout << "ivec.size()" << ivec.size() << std::endl;
    return 0;
}