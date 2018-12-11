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
    bucket = new list<ProjLineItem *>[bucket_size];
}

void hash_table::insertItem(ProjLineItem *projLineItem) {
    int index = hashFunction((*projLineItem).order_key);
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
    std::vector<ProjLineItem> global_results;
#if PARELLEL
#pragma omp parallel
    {
#endif
        std::vector<ProjLineItem> private_results;
#if PARELLEL
#pragma omp for
#endif
        for (int index = 0; index < bucket_size; ++index) {
            if (bucket[index].size() > 0) {
                std::unordered_map<int, int> map;
                ProjLineItem projlineitem;
                for (auto i = bucket[index].begin(); i != bucket[index].end(); ++i) {
                    std::unordered_map<int, int>::iterator j = map.find((*i)->order_key);
                    if (j != map.end()) {
                        j->second += (*i)->quantity;
                    } else {
                        map.insert(std::pair<int, int>((*i)->order_key, (*i)->quantity));
                    }
                }
                for (auto k = map.begin(); k != map.end(); ++k) {
                    projlineitem.order_key = k->first;
                    projlineitem.quantity = k->second;
                    private_results.push_back(projlineitem);
                }
            }
        }
#if PARELLEL
        for (int t = 0; t < omp_get_num_threads(); t++) {
#pragma omp barrier
            if (t == omp_get_thread_num()) {
//#pragma omp critical
                global_results.insert(global_results.end(), private_results.begin(), private_results.end());
            }
        }
    }
    std::cout << global_results.size() << std::endl;
#endif
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
