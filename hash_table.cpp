//
// Created by he on 12/7/18.
//

#include "hash_table.h"

int hash_table::hashFunction(int key) {
    return (key % bucket_size);
}


hash_table::hash_table(int bucket_size) {
    this->bucket_size = bucket_size;
    bucket = new list<ProjLineItem *>[bucket_size];
}

void hash_table::insertItem(ProjLineItem *projLineItem) {
    int index = hashFunction((*projLineItem).order_key);
#pragma omp critical
    bucket[index].push_back(projLineItem);
}

//#pragma omp parallel for
int hash_table::count() {
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
    return count;
}

vector<ProjLineItem> hash_table::sum() {
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
                global_results.insert(global_results.end(), private_results.begin(), private_results.end());
            }
        }
    }
    std::cout << global_results.size() << std::endl;
#endif
    return global_results;
}


void hash_table::distributed_sum() {
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
                global_results.insert(global_results.end(), private_results.begin(), private_results.end());
            }
        }
    }
    std::cout << global_results.size() << std::endl;
#endif
}



// efficiency is lower than one thread mode
//void hash_table::para_insertItem(vector<ProjLineItem> *items) {
//#if PARELLEL
//#pragma omp parallel
//    {
//#endif
//        list<ProjLineItem *> *private_bucket = new list<ProjLineItem *>[bucket_size];
//#if PARELLEL
//#pragma omp for
//#endif
//        for (int i = 0; i < items->size(); ++i) {
//            int index = hashFunction((*items)[i].order_key);
//            private_bucket[index].push_back(&(*items)[i]);
//        }
//#if PARELLEL
//        for (int t = 0; t < omp_get_num_threads(); t++) {
//#pragma omp barrier
//            if (t == omp_get_thread_num()) {
//                for (int j = 0; j < bucket_size; ++j) {
//                    if (private_bucket[j].size() > 0) {
//                        bucket[j].merge(private_bucket[j]);
//                    }
//                }
//            }
//        }
//    }
//#endif
//}