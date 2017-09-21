/*
  *  NETWORK_H
  *   基于ZEROMQ建立的通信机制
  */

#ifndef _NETWORK_H
#define _NETWORK_H
#include "common.h"

//初始化zeromq
void*
zmq_setContext() ;

void
zmq_closeContext(void *context) ;

void*
zmq_Client_setSocket(void* context, int type, string host, int port) ;

void*
zmq_Server_setSocket(void* context, int type, string host, int port) ;

void
zmq_closeSocket(void* socket) ;

//zeromq发送数据
int
zmq_sendStream(void* socket, string s, int length) ;

int
zmq_sendByte(string buf, int length, byte b) ;

int
zmq_sendString(string buf,  string msg, int length) ;

int
zmq_sendInt(string buf, int length, int msg) ;

//zeromq获取数据
int
zmq_sendFloat(string buf, int length, float msg) ;

void
zmq_recvStream(void* socket, void* buf) ;

void
zmq_recvString(void* buf , int* index, string str);

int
zmq_recvInt(void* buf, int* index) ;

float
zmq_recvFloat(void* socket, int* Index) ;

//command client
 string
 Client_REQ(string host, int port, string msg) ;

int
Server_DealerandRouter_broker(string host, int routerPort, int dealerPort) ;


int
Server_DealerandRouter_worker(string host, int port, string (*f)(string msg)) ;


#endif // _NETWORK_H
