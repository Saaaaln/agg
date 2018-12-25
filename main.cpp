#include <iostream>
#include <vector>
#include <sys/time.h>
#include "omp.h"
#include "line_item_parser.h"
#include "proj_data.h"
#include "hash_table.h"

void prepare() {
    timeval t1, t2;
}

int main() {

//    proj_tpch1();

//    proj_tpch10();

    struct timeval t1, t2;
    int deltaT = 0;
    double total = 0;
    gettimeofday(&t1, NULL);
    std::vector<ProjLineItem *> *items = ReadAndParseProjLineItems("/home/he/Desktop/proj_lineitem_sf1.tbl");
//    std::vector<ProjLineItem> *items = ReadAndParseProjLineItems("/home/claims/zyhe/data/proj_lineitem_sf50.tbl");
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "--------load table into memory costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;
    total += deltaT * 1.0 / 1000;
    //build hash table
    //1048576 or  524288
    hash_table *ht = new hash_table(HASH_SIZE);
    gettimeofday(&t1, NULL);
//#pragma omp parallel for  //parallel may cause push_back of vector error
    for (int i = 0; i < items->size(); ++i) {
        ht->insertItem((*items)[i]);
    }
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "--------build hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;
    total += deltaT * 1.0 / 1000;

    int choice = 0;
    std::cout << "input choice(1 is count(), 2 is sum()):" << std::endl;
    std::cin >> choice;
    if (choice == 1) {
        //select COUNT(L_ORDERDEY) from LINEITEM
        gettimeofday(&t1, NULL);
        ht->count();
        gettimeofday(&t2, NULL);
        deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
        std::cout << "--------travel hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;
        total += deltaT * 1.0 / 1000;
    } else if (choice == 2) {
        //select L_ORDERDEY, SUM(L_QUANTITY) from LINEITEM GROUP BY L_ORDERKEY
        gettimeofday(&t1, NULL);
        ht->sum();
        gettimeofday(&t2, NULL);
        deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
        std::cout << "--------merge hash table costs " << deltaT * 1.0 / 1000 << " ms" << std::endl;
        total += deltaT * 1.0 / 1000;
    }
    std::cout << "--------time costs " << total << " ms" << std::endl;
}
