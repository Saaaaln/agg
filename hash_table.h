//
// Created by he on 12/7/18.
//

#ifndef AGGREGATION_HASH_TABLE_H
#define AGGREGATION_HASH_TABLE_H

#include <list>
#include <unordered_map>
#include "omp.h"
#include "line_item_parser.h"

#define DEBUG 0
#define PARELLEL 1
#define HASH_SIZE 1048576 //or 524288

using namespace std;

class hash_table {

public:

    hash_table(int bucket_size);

    void insertItem(ProjLineItem *projLineItem);

    void para_insertItem(vector<ProjLineItem> *items);

    int hashFunction(int x);

    int count();

    vector<ProjLineItem> sum();

    void distributed_count();

    void distributed_sum();


    static bool sortHashBucket(const ProjLineItem &p1, const ProjLineItem &p2) {
        return p1.order_key < p2.order_key;
    }

    int bucket_size;

    list<ProjLineItem *> *bucket;
};


#endif //AGGREGATION_HASH_TABLE_H
