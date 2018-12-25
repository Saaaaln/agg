//
// Created by he on 12/21/18.
//

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <iostream>
#include <vector>
#include <list>
#include <string.h>
#include <unistd.h>
#include "net_buffer.h"
#include "line_item_parser.h"
#include "hash_table.h"
#include "omp.h"

static int node_id = 0;

#if W_DEBUG
static int write_no = 0;
static int total_nums = 0;
#endif

int connect() {
    //build connect
    struct addrinfo hint, *result;
    int res, sfd;

    memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_INET;
    hint.ai_socktype = SOCK_STREAM;

#if MODE
    res = getaddrinfo("10.11.6.104", "8089", &hint, &result);
#else
    res = getaddrinfo("127.0.0.1", "8089", &hint, &result);
//    res = getaddrinfo("127.0.0.1", "8080", &hint, &result);
#endif

    if (res == -1) {
        perror("error : cannot get socket address!\n");
        exit(1);
    }

    sfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (sfd == -1) {
        perror("error : cannot get socket file descriptor!\n");
        exit(1);
    }

    res = connect(sfd, result->ai_addr, result->ai_addrlen);
    if (res == -1) {
        perror("error : cannot connect the server!\n");
        exit(1);
    }

    return sfd;
}

#if W_DEBUG
void showblock(char *block) {
    int node;
    int data_num;
    memcpy(&node, block, 4);
    memcpy(&data_num, block + 4, 4);
    total_nums += data_num;
    std::cout << ",write_no:" << node << ",data num:" << data_num << ",write tuples:" << total_nums << std::endl;
}
#endif

bool sendbuf(NetBuffer *netbuffer) {
    if (!netbuffer->ilist.empty()) {
        char *front = netbuffer->ilist.front();
        int ret = write(netbuffer->sfd, front, BLOCK_SIZE);
#if W_DEBUG
        std::cout << "ret:" << ret;
        showblock(front);
#endif
        netbuffer->ilist.pop_front();
        delete[]front;
        return true;
    } else {
        return false;
    }
};

void producer(std::vector<ProjLineItem *> *ivec, NetBuffer *netbuffer) {
    int tuple_nums = ivec->size();
    int pos_at_block = 0;
    int count = 0;
    int start = 0;
    int end = ivec->size();
    int size = end - start;
    int block_data_size = BLOCK_SIZE - 8;
    char block[block_data_size];
    memset(block, '\0', BLOCK_SIZE);
    for (int i = start; i != end; ++i) {
        memcpy(block + pos_at_block * TUPLE_SIZE, (*ivec)[i], TUPLE_SIZE);
        ++pos_at_block;
        ++count;

        if ((count != 0 && count % BLOCK_TUPLE == 0) || count == size) {
            int node = 0;
            int data_num = pos_at_block;

            //putbuf
            char *send_block = new char[BLOCK_SIZE];
            memcpy(send_block, &node, 4);
#if W_DEBUG
            memcpy(send_block, &write_no, 4);
            memcpy(send_block + 4, &data_num, 4);
            memcpy(send_block + 8, block, BLOCK_SIZE - 8);
            netbuffer->ilist.push_back(send_block);
            ++write_no;
#else
            memcpy(send_block, &node, 4);
            memcpy(send_block + 4, &data_num, 4);
            memcpy(send_block + 8, block, block_data_size);
            netbuffer->ilist.push_back(send_block);
#endif
            memset(block, '\0', block_data_size);
            pos_at_block = 0;
        }
    }
//#pragma omp parallel
//    {
//        int tuple_nums = ivec->size();
//        int pos_at_block = 0;
//        int count = 0;
//        int total_threads = omp_get_num_threads();
//        int thread_num = omp_get_thread_num();
//        int start = thread_num * tuple_nums / total_threads;
//        int end = (thread_num + 1) * tuple_nums / total_threads;
//        int size = end - start;
//        int block_data_size = BLOCK_SIZE - 8;
//        char block[block_data_size];
//        memset(block, '\n', block_data_size);
//        if (size == (tuple_nums / total_threads)) {
//            for (int i = start; i != end; ++i) {
//                memcpy(block + pos_at_block * TUPLE_SIZE, (*ivec)[i], TUPLE_SIZE);
//                ++pos_at_block;
//                ++count;
//
//                if ((count != 0 && count % BLOCK_TUPLE == 0) || count == size) {
//                    int node = 0;
//                    int data_num = pos_at_block;
//
//                    //putbuf
//                    omp_set_lock(&(netbuffer->insert_mutex));
//                    char *send_block = new char[BLOCK_SIZE];
////                    memcpy(send_block, &node, 4);
//                    memcpy(send_block, &write_no, 4);
//                    memcpy(send_block + 4, &data_num, 4);
//                    memcpy(send_block + 8, block, block_data_size);
//                    netbuffer->ilist.push_back(send_block);
//                    ++write_no;
//                    omp_unset_lock(&(netbuffer->insert_mutex));
//
//                    memset(block, '\n', block_data_size);
//                    pos_at_block = 0;
//                }
//            }
//        } else {
//            for (int i = start; i != end; ++i) {
//                memcpy(block + pos_at_block * TUPLE_SIZE, &(*ivec)[i], TUPLE_SIZE);
//                ++pos_at_block;
//                ++count;
//                if ((count != 0 && count % BLOCK_TUPLE == 0) || count == size) {
//                    int node = 1;
//                    int data_num = pos_at_block;
//
//                    //putbuf
//                    omp_set_lock(&(netbuffer->insert_mutex));
//                    char *send_block = new char[BLOCK_SIZE];
////                    memcpy(send_block, &node, 4);
//                    memcpy(send_block, &write_no, 4);
//                    memcpy(send_block + 4, &data_num, 4);
//                    memcpy(send_block + 8, block, BLOCK_SIZE - 8);
//                    netbuffer->ilist.push_back(send_block);
//#if DEBUG
//                    ++write_no;
//#endif
//                    omp_unset_lock(&(netbuffer->insert_mutex));
//
//                    memset(block, '\n', BLOCK_SIZE - 8);
//                    pos_at_block = 0;
//                }
//            }
//        }
//    }
    netbuffer->done = 1;
}


void count_producer(hash_table *ht, NetBuffer *netbuffer) {
    int node = node_id;
    int data_num = ht->count();
    //putbuf
    char *send_block = new char[BLOCK_SIZE];
    memcpy(send_block, &node, 4);
    memcpy(send_block + 4, &data_num, 4);
    netbuffer->ilist.push_back(send_block);
    std::cout << " ";
    netbuffer->done = 1;
}


void sum_producer(hash_table *ht, NetBuffer *netbuffer) {
#pragma omp parallel
    {
        int hash_size = HASH_SIZE;
        int pos_at_block = 0;
        int count = 0;
        int total_threads = omp_get_num_threads();
        int thread_num = omp_get_thread_num();
        int start = thread_num * hash_size / total_threads;
        int end = (thread_num + 1) * hash_size / total_threads;
        int block_data_size = BLOCK_SIZE - 8;
        char block[block_data_size];
        memset(block, '\0', block_data_size);
        for (int index = start; index != end; ++index) {
            if (ht->bucket[index].size() > 0) {
                std::unordered_map<int, int> map;
                for (auto i = ht->bucket[index].begin(); i != ht->bucket[index].end(); ++i) {
                    std::unordered_map<int, int>::iterator j = map.find((*i)->order_key);
                    if (j != map.end()) {
                        j->second += (*i)->quantity;
                    } else {
                        map.insert(std::pair<int, int>((*i)->order_key, (*i)->quantity));
                    }
                }
                for (auto k = map.begin(); k != map.end(); ++k) {
                    ProjLineItem projlineitem;
                    projlineitem.order_key = k->first;
                    projlineitem.quantity = k->second;
                    memcpy(block + pos_at_block * TUPLE_SIZE, &projlineitem, TUPLE_SIZE);
                    ++pos_at_block;
                    ++count;
                    if ((count != 0 && count % BLOCK_TUPLE == 0)) {
                        //putbuf
                        omp_set_lock(&(netbuffer->insert_mutex));
                        char *send_block = new char[BLOCK_SIZE];
#if W_DEBUG
                        memcpy(send_block, &write_no, 4);
                        memcpy(send_block + 4, &pos_at_block, 4);
                        memcpy(send_block + 8, block, block_data_size);
                        netbuffer->ilist.push_back(send_block);
//                        std::cout << "push thread_num:" << thread_num << ", data_num" << data_num << std::endl;
                        ++write_no;
#else
                        memcpy(send_block, &node_id, 4);
                        memcpy(send_block + 4, &pos_at_block, 4);
                        memcpy(send_block + 8, block, block_data_size);
                        netbuffer->ilist.push_back(send_block);
#endif
//                        memcpy(send_block, &node_id, 4);
                        omp_unset_lock(&(netbuffer->insert_mutex));

                        memset(block, '\0', block_data_size);
                        pos_at_block = 0;
                    }
                }
            }
            if (index == (end - 1)) {
                omp_set_lock(&(netbuffer->insert_mutex));
                char *send_block = new char[BLOCK_SIZE];
#if W_DEBUG
                memcpy(send_block, &write_no, 4);
                memcpy(send_block + 4, &pos_at_block, 4);
                memcpy(send_block + 8, block, block_data_size);
                netbuffer->ilist.push_back(send_block);
                ++write_no;
#else
                memcpy(send_block, &node_id, 4);
                memcpy(send_block + 4, &pos_at_block, 4);
                memcpy(send_block + 8, block, block_data_size);
                netbuffer->ilist.push_back(send_block);
#endif
                omp_unset_lock(&(netbuffer->insert_mutex));

                memset(block, '\n', BLOCK_SIZE);
                pos_at_block = 0;
            }
        }
    }

    int end = -1;
    char *end_message = new char[BLOCK_SIZE];
    memset(end_message, '\0', BLOCK_SIZE);
    memcpy(end_message, &node_id, 4);
    memcpy(end_message + 4, &end, 4);
    netbuffer->ilist.push_back(end_message);

    std::cout << " ";
    netbuffer->done = 1;
}

void consumer(NetBuffer *netbuffer) {
    while (true) {
//        receive();
        if (sendbuf(netbuffer)) {
        } else if (netbuffer->done == 1) {
            break;
        }
    }
}

int main() {
    int choice = 0;

    NetBuffer *netbuffer = new NetBuffer();
    omp_init_lock(&netbuffer->insert_mutex);

//    char buf[BLOCK_SIZE];
    std::vector<ProjLineItem *> pvec;

    int sfd = connect();

    netbuffer->sfd = sfd;

    char buf[4];
    memset(buf, '\0', 4);

    while (true) {
        int cnt = read(sfd, buf, 10);
        if (cnt != -1 && cnt != 0) {
            memcpy(&node_id, buf, 4);
            std::cout << "node is " << node_id;
            memcpy(&choice, buf + 4, 4);
            if (choice == 1) std::cout << " and distributed count begin!" << std::endl;
            else if (choice == 2) std::cout << " and distributed sum begin!" << std::endl;
            else if (choice == 3) std::cout << " and test begin!" << std::endl;
            else
                std::cout << " and command is error!" << std::endl;
            break;
        }
    }


    //load data in memory
#if MODE
    std::vector<ProjLineItem *> *items = ReadAndParseProjLineItems("/home/claims/zyhe/data/proj_lineitem_sf1.tbl");
#else
    std::vector<ProjLineItem *> *items = ReadAndParseProjLineItems("/home/he/Desktop/proj_lineitem_sf1.tbl");
#endif
    std::cout << "load data complete" << std::endl;

    //build hash table
    //1048576 or  524288
    hash_table *ht = new hash_table(HASH_SIZE);
    for (int i = 0; i < items->size(); ++i) {
        ht->insertItem((*items)[i]);
    }
    std::cout << "build hash table complete" << std::endl;

    if (choice == 1) {
        //select COUNT(L_ORDERDEY) from LINEITEM
#pragma omp parallel
        {
            omp_set_dynamic(0);
            omp_set_nested(1);
            omp_set_num_threads(2);
#pragma omp sections
            {
#pragma omp section
                count_producer(ht, netbuffer);
#pragma omp section
                consumer(netbuffer);
            }
        }
        std::cout << "local agg and send to coordinator complete" << std::endl;
    } else if (choice == 2) {
        //select L_ORDERDEY, SUM(L_QUANTITY) from LINEITEM GROUP BY L_ORDERKEY
#pragma omp parallel
        {
            omp_set_dynamic(0);
            omp_set_nested(1);
            omp_set_num_threads(2);
#pragma omp sections
            {
#pragma omp section
                sum_producer(ht, netbuffer);
#pragma omp section
                consumer(netbuffer);
            }
        }
#if W_DEBUG
        std::cout << "total write data:" << total_nums << std::endl;
#endif
        std::cout << "local sum and send to coordinator complete" << std::endl;
    } else if (choice == 3) {
        //select L_ORDERDEY, SUM(L_QUANTITY) from LINEITEM GROUP BY L_ORDERKEY
#pragma omp parallel
        {
            omp_set_dynamic(0);
            omp_set_nested(0);
            omp_set_num_threads(2);
#pragma omp sections
            {
#pragma omp section
                producer(items, netbuffer);
#pragma omp section
                consumer(netbuffer);
            }
        }
    }

    for (int i = 0; i < items->size(); ++i) {
        delete (*items)[i];
    }
    delete items;
    delete ht;
    close(sfd);
    return 0;
}