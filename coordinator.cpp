//
// Created by he on 12/21/18.
//

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <iostream>
#include <vector>
#include "net_buffer.h"
#include "line_item_parser.h"

#define MAX_EVENTS 64

static int epoll = 0;
static int sd = 0;
static int sfd = 0;
struct timeval t1, t2;
static int read_tuples = 0;

static int create_and_bind(const char *port);

static int make_socket_non_binding(int sfd);

static int create_and_bind(const char *port) {
    struct addrinfo hint, *result;
    int res, sfd;

    memset(&hint, 0, sizeof(struct addrinfo));
    hint.ai_family = AF_INET;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_flags = AI_PASSIVE;

    res = getaddrinfo(NULL, port, &hint, &result);
    if (res == -1) {
        perror("error : can not get address info\n");
        exit(1);
    }

    sfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (sfd == -1) {
        perror("error : can not get socket descriptor!\n");
        exit(1);
    }


    res = bind(sfd, result->ai_addr, result->ai_addrlen);
    if (res == -1) {
        perror("error : can not bind the socket!\n");
        exit(1);
    }

    freeaddrinfo(result);

    return sfd;
}


static int make_socket_non_binding(int sfd) {
    int flags, res;

    flags = fcntl(sfd, F_GETFL);
    if (flags == -1) {
        perror("error : cannot get socket flags!\n");
        exit(1);
    }

    flags |= O_NONBLOCK;
    res = fcntl(sfd, F_SETFL, flags);
    if (res == -1) {
        perror("error : cannot set socket flags!\n");
        exit(1);
    }

    return 0;
}

std::vector<ProjLineItem *> *dowork(int choice, epoll_event event, epoll_event events[MAX_EVENTS]) {
    int cnt = 0;
    int res = 0;
    int node = 0;
    int rec_node = 0;
    int count_result = 0;
    std::vector<int> ifd_;
    std::vector<ProjLineItem *> *results = new std::vector<ProjLineItem *>;
    while (1) {
        cnt = epoll_wait(epoll, events, MAX_EVENTS, -1);

        for (int i = 0; i < cnt; i++) {

            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP)
                || !(events[i].events & EPOLLIN)) {
                perror("error : socket fd error!\n");
                close(events[i].data.fd);
                continue;
            } else if (events[i].data.fd == sfd) {
                while (1) {
                    struct sockaddr client_addr;
                    unsigned int addrlen = sizeof(struct sockaddr);
                    sd = accept(sfd, &client_addr, &addrlen);
                    if (sd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        } else {
                            perror("error : cannot accept new socket!\n");
                            continue;
                        }

                    }
//                    res = make_socket_non_binding(sd);
                    if (res == -1) {
                        perror("error : cannot set flags!\n");
                        exit(1);
                    }
                    event.data.fd = sd;
//                    event.events = EPOLLET | EPOLLIN;
                    event.events = EPOLLIN;
                    res = epoll_ctl(epoll, EPOLL_CTL_ADD, sd, &event);
                    ++node;
//                    std::cout << "node:" << sfd << std::endl;          //listen socket
//                    std::cout << "node:" << events[i].data.fd << std::endl; //listen socket
                    struct sockaddr_in *sock = (struct sockaddr_in *) &client_addr;
                    std::cout << inet_ntoa(sock->sin_addr) << " connected, node number is " << event.data.fd
                              << std::endl; //connect socket
                    ifd_.push_back(event.data.fd);
                    if (node == NODE_NUM && !ifd_.empty()) {
                        for (int j = 0; j < ifd_.size(); ++j) {
                            char message[8];
                            memcpy(message, &ifd_[j], 4);
                            memcpy(message + 4, &choice, 4);
                            write(event.data.fd, message, 8);
                        }
                        node = 0;
                        std::vector<int>().swap(ifd_);
                        gettimeofday(&t1, NULL);
                    }
                }
            } else {
                char buf[BLOCK_SIZE];
                int read_size = 0;
                int rest_size = BLOCK_SIZE;
                while (1) {
                    int ret = read(events[i].data.fd, buf + read_size, rest_size);
                    if (ret == -1) {
                        if (errno == EAGAIN) {
                            break;
                        }
                        perror("error : read error!\n");
                        exit(1);
                    } else if (ret == 0) {
                        close(events[i].data.fd);
                        break;
                    }
                    read_size += ret;
                    rest_size = BLOCK_SIZE - read_size;
                    if (rest_size == 0) {
                        if (choice == 1) {
                            int node;
                            int data_num;
                            memcpy(&node, buf, 4);
                            memcpy(&data_num, buf + 4, 4);
                            std::cout << "node:" << node << ",count(*):" << data_num << std::endl;
                            ++rec_node;
                            count_result += data_num;
                            if (rec_node == NODE_NUM) {
                                gettimeofday(&t2, NULL);
                                int deltaT = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
                                std::cout << "--------cost time: " << deltaT * 1.0 / 1000 << " ms" << std::endl;
                                return results;
                            }
                        } else if (choice == 2) {
                            int node;
                            int data_num;
                            memcpy(&node, buf, 4);
                            memcpy(&data_num, buf + 4, 4);
                            read_tuples += data_num;
                            std::cout << "read_no: " << node << ", data num:" << data_num << ", read_tuples:"
                                      << read_tuples << std::endl;

//                            for (int j = 0; j < data_num; ++j) {
//                                ProjLineItem *proj = new ProjLineItem;
//                                memcpy(proj, buf + TUPLE_SIZE * (j + 1), TUPLE_SIZE);
//                                std::cout << "ProjLineItem:" << proj->order_key << "," << proj->quantity << std::endl;
//                            }
//                            std::cout << "results:" << results->size() << std::endl;
                        } else if (choice == 3) {
                            int node;
                            int data_num;
                            memcpy(&node, buf, 4);
                            memcpy(&data_num, buf + 4, 4);
                            read_tuples += data_num;
                            std::cout << "ret:" << ret << ",read_no: " << node << ", data num:" << data_num
                                      << ", read_tuples:" << read_tuples << std::endl;

//                            ProjLineItem *proj = new ProjLineItem;
//                            memcpy(proj, buf + 8, TUPLE_SIZE);
//                            std::cout << "order_key:" << proj->order_key << ",quantity :" << proj->quantity
//                                      << std::endl;

//                            for (int j = 0; j < data_num; ++j) {
//                                ProjLineItem *proj = new ProjLineItem;
//                                memcpy(proj, buf + TUPLE_SIZE * (j + 1), TUPLE_SIZE);
//                                std::cout << "ProjLineItem:" << proj->order_key << "," << proj->quantity << ",block at"
//                                          << TUPLE_SIZE * (j + 1) << std::endl;
//                            }
                        }
                        read_size = 0;
                        rest_size = BLOCK_SIZE;
                    }
                }
            }
        }
    }
}

int main() {
    int choice = 0;
    std::cout << "input choice(1 is dis_count(), 2 is dis_sum()):" << std::endl;
    std::cin >> choice;

    int deltaT = 0;

    //build listen and epoll
    int res, i;
    struct epoll_event event, events[MAX_EVENTS];
    const char *ip = "8089";

    sfd = create_and_bind(ip);
    if (sfd == -1) {
        perror("error : cannot create socket!\n");
        exit(1);
    }

    res = make_socket_non_binding(sfd);
    if (res == -1) {
        perror("error : connot set flags!\n");
        exit(1);
    }

    res = listen(sfd, SOMAXCONN);
    if (res == -1) {
        perror("error : cannot listen!\n");
        exit(1);
    }

    epoll = epoll_create(1);
    if (epoll == -1) {
        perror("error : cannot create epoll!\n");
        exit(1);
    }

//    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    event.events = EPOLLIN | EPOLLOUT;
    event.data.fd = sfd;
    res = epoll_ctl(epoll, EPOLL_CTL_ADD, sfd, &event);
    if (res == -1) {
        perror("error : can not add event to epoll!\n");
        exit(1);
    }

    dowork(choice, event, events);

    close(sfd);
}

