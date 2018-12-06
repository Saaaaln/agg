#include <iostream>
#include <vector>
#include <sys/time.h>
#include <fstream>
#include "omp.h"
//#include "table.h"
#include "line_item_parser.h"
#include "proj_data.h"

int main() {

//    proj_tpch1();

    proj_tpch10();

//    struct timeval t1, t2;
//    int deltaT = 0;
//
//    gettimeofday(&t1, NULL);
//    std::cout << "begin" << std::endl;
//    std::vector<ProjLineItem> &items = ReadAndParseProjLineItems("/home/he/Desktop/proj_lineitem_sf10.tbl");
//    gettimeofday(&t2, NULL);
//    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//    printf("--------load table is over costs time (ms) = %lf\n", deltaT * 1.0 / 1000);
//
//    std::cout << "hallo?" << std::endl;
//    gettimeofday(&t1, NULL);
//    std::cout << items.size() << std::endl;
//    gettimeofday(&t2, NULL);
//    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//    printf("--------count (*) costs time (ms) = %lf\n", deltaT * 1.0 / 1000);

    return 0;
}