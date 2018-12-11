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
    std::vector<ProjLineItem> *items = ReadAndParseProjLineItems("/home/he/Desktop/proj_lineitem_sf10.tbl");
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "--------load table into memory costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;

    //1048576 or  524288
    hash_table *ht = new hash_table(1048576);

    gettimeofday(&t1, NULL);

    //build hash table
#pragma omp parallel for
    for (int i = 0; i < items->size(); ++i) {
        ht->insertItem(&(*items)[i]);
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
    std::cout << "--------merge hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;

//    gettimeofday(&t1, NULL);
////    ht->sum();
//    ht->travel1();
//    gettimeofday(&t2, NULL);
//    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//    std::cout << "--------critical hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;




//    struct timeval t1, t2;
//    std::vector<int> global_vec;
//    gettimeofday(&t1, NULL);
//#pragma omp parallel
//    {
//        std::vector<int> local_vec;
//#pragma omp for
//        for (int i = 0; i < 100; i++) {
//            // some computations
//            local_vec.push_back(i);
//        }
//        for (int t = 0; t < omp_get_num_threads(); t++) {
////#pragma omp barrier
//            if (t == omp_get_thread_num()) {
//#pragma omp critical
//                global_vec.insert(global_vec.end(), local_vec.begin(), local_vec.end());
//            }
//        }
//    }
//    gettimeofday(&t2, NULL);
//    int deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//    std::cout << "--------travel hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;
}
