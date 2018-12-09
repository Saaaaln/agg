//
// Created by he on 12/7/18.
//

#ifndef AGGREGATION_HASH_TABLE_H
#define AGGREGATION_HASH_TABLE_H

#include <list>
#include "omp.h"
#include "line_item_parser.h"

using namespace std;

class hash_table {
    int bucket_size;

    list<ProjLineItem> *bucket;

    omp_lock_t lock;

public:

    hash_table(int bucket_size);

    void insertItem(ProjLineItem projLineItem);

    int hashFunction(int x);

    void count();

    void sum();

    static bool sortHashBucket(const ProjLineItem &p1, const ProjLineItem &p2) {
        return p1.order_key < p2.order_key;
    }

};


#endif //AGGREGATION_HASH_TABLE_H
