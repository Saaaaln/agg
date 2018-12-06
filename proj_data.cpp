//
// Created by he on 12/6/18.
//

#include "proj_data.h"
#include "line_item_parser.h"

void proj_tpch1() {
    struct timeval t1, t2;
    int deltaT = 0;

    gettimeofday(&t1, NULL);
    std::cout << "begin" << std::endl;
    std::vector<LineItem> &items = ReadAndParseLineItems("/home/he/Desktop/lineitem1.tbl");
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    printf("--------load table is over costs time (ms) = %lf\n", deltaT * 1.0 / 1000);

//#pragma omp parallel
    std::cout << "hallo?" << std::endl;
    gettimeofday(&t1, NULL);
    std::cout << items.size() << std::endl;
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    printf("--------count (*) costs time (ms) = %lf\n", deltaT * 1.0 / 1000);

    //project lineitem(int order_key, double quantity)
    std::ofstream dataFile;
    dataFile.open("/home/he/Desktop/tpch_sf1/proj_lineitem_sf1.tbl", std::ofstream::app);

    int count = 0;
    gettimeofday(&t1, NULL);
    for (int i = 0; i < items.size(); ++i) {
        dataFile << items[i] << std::endl;
        ++count;
    }
    gettimeofday(&t2, NULL);
    deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
    std::cout << "count:" << count << std::endl;
    dataFile.close();
}

void proj_tpch10() {
    std::cout << "begin" << std::endl;
    ReadAndProjAllLineItems("/home/he/Desktop/lineitem100.tbl");
}
