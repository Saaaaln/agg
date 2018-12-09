//
// Created by he on 12/7/18.
//

#include "hash_table.h"

#define DEBUG 0
#define PARELLEL 1

int hash_table::hashFunction(int key) {
    return (key % bucket_size);
}


hash_table::hash_table(int bucket_size) {
    this->bucket_size = bucket_size;
    bucket = new list<ProjLineItem>[bucket_size];
}

void hash_table::insertItem(ProjLineItem projLineItem) {
    int index = hashFunction(projLineItem.order_key);
    bucket[index].push_back(projLineItem);
}

//#pragma omp parallel for
void hash_table::count() {
    int count = 0;

#if PARELLEL
#pragma omp parallel for reduction(+:count)
#endif
    for (int i = 0; i < bucket_size; ++i) {
        if (bucket[i].size() > 0) {
#if DEBUG
            std::cout << "index:" << i << std::endl;
            std::cout << "bucket[i].size():" << bucket[i].size() << std::endl;
#endif
            count += bucket[i].size();
        }
    }
    std::cout << "count(*):" << count << std::endl;
}

void hash_table::sum() {
    static std::vector<ProjLineItem> results;
#if PARELLEL
#pragma omp parallel for
#endif
    for (int index = 0; index < bucket_size; ++index) {
        int order_key_before = -999;
//        int this_thread = omp_get_thread_num();
        if (bucket[index].size() > 0) {
            ProjLineItem projlineitem;
            int controller = 0;
            bucket[index].sort(sortHashBucket);
#if DEBUG
            auto j = bucket[index].begin();
            for (; j != bucket[index].end(); ++j) {
                std::cout << "order_key:" << (*j).order_key << std::endl;
            }
#endif
            for (auto i = bucket[index].begin(); i != bucket[index].end(); ++i) {
                projlineitem.order_key = (*i).order_key;
                if (i == bucket[index].begin()) {
                    order_key_before = projlineitem.order_key;
                }
                if (order_key_before == (*i).order_key) {
                    projlineitem.quantity += (*i).quantity;
                } else {
                    int mid = projlineitem.order_key;
                    projlineitem.order_key = order_key_before;
#if PARELLEL
                    //                    omp_set_lock(&lock);
#endif
#pragma omp critical
                    results.push_back(projlineitem);
#if PARELLEL
                    //                    omp_unset_lock(&lock);
#endif
                    order_key_before = mid;
                    projlineitem.order_key = (*i).order_key;
                    projlineitem.quantity = (*i).quantity;
                }
                ++controller;
#if DEBUG
                std::cout << "bucket[index].order_key:" << (*i).order_key << std::endl;
                std::cout << "bucket[index].quantity:" << (*i).quantity << std::endl;
                std::cout << "projlineitem.order_key:" << projlineitem.order_key << std::endl;
                std::cout << "projlineitem.quantity:" << projlineitem.quantity << std::endl;
#endif
                if (controller == bucket[index].size() && order_key_before == (*i).order_key) {
#if PARELLEL
                    //                    omp_set_lock(&lock);
#endif
#pragma omp critical
                    results.push_back(projlineitem);
#if PARELLEL
                    //                    omp_unset_lock(&lock);
#endif
                    projlineitem.quantity = 0;
                }
#if DEBUG
                std::cout << "result.size:" << results.size() << std::endl;
#endif
            }
        }
    }
    std::cout << "result.size:" << results.size() << std::endl;
}


//get keys
//    int index = hashFunction((*items)[i].order_key);
//    std::cout << "index:" << index << std::endl;
//    std::cout << "order_key:" << (*items)[i].order_key << std::endl;
//    std::cout << "quantity:" << (*items)[i].quantity << std::endl;
//    if (bucket[index].size() > 0) {
//        std::cout << "bucket[index].size():" << bucket[index].size() << std::endl;
//        projlineitem.order_key = (*items)[i].order_key;
//        for (auto j = bucket[index].begin(); j != bucket[index].end(); ++j) {
//            std::cout << "quantity:" << (*j).quantity << std::endl;
//            if (*j == (*items)[i]) {
//                projlineitem.quantity += (*items)[i].quantity;
//            }
//        }
//        results.push_back(projlineitem);
//    }
//
//    std::cout << "result.size:" << results.size() << std::endl;


//
//    // find the key in (inex)th list
//    list <int> :: iterator i;
//    for (i = table[index].begin();
//         i != table[index].end(); i++) {
//        if (*i == key)
//            break;
//    }
//
//    // if key is found in hash table, remove it
//    if (i != table[index].end())
//        table[index].erase(i);
