//
// Created by he on 12/21/18.
//

#ifndef AGGREGATION_NET_BUFFER_H
#define AGGREGATION_NET_BUFFER_H

#include <list>
#include "omp.h"

#define MAX_EVENTS 64

#define TUPLE_SIZE 8

#define BLOCK_TUPLE 1023
#define BLOCK_SIZE 8192

#define NODE_NUM 1  //worker node numbers
#define MODE 0 //0 is remote, 1 is local
#define W_DEBUG 0  //connect node numbers




//#define BLOCK_TUPLE 127
//#define BLOCK_SIZE 1024

//#define BLOCK_TUPLE 2047
//#define BLOCK_SIZE 16384 //64*1024=65536 =BLOCK_TUPLE*8+8

//#define BLOCK_TUPLE 4095
//#define BLOCK_SIZE 32768 //64*1024=65536 =BLOCK_TUPLE*8+8

struct NetBuffer {
    std::list<char *> ilist;
    int done = 0;
    int sfd = 0;
    omp_lock_t insert_mutex;
};


#endif //AGGREGATION_NETWORK_H
