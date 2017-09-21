/*
 * NETWORK_C
 * 基于ZEROMQ的底层数据传输的实现
 */
#include "network.h"
#include <zmq.h>
#include <unistd.h>
#include "postgres.h"

/*
 * makeUrl
 * 根据已有的IP地址生成符合ZEROMQ规则的连接url
 * url: 用于连接的url
 * host: 指定要连接的服务器的ip地址
 * port ：服务器的端口号
 */
void
makeUrl (string url , string host, int port) {
    string_init(url , "tcp://");
    string_contat(url,  3, host, ":", intToString(port));
}

/*
 * zmq_setContext
 * 创建zmq连接的上下文
 */
void*
zmq_setContext() {
    void* context = NULL;
    if ((context = zmq_ctx_new()) == NULL){
        elog(INFO, "context failed");
        zmq_closeContext(context) ;
        exit(1);
    }
    return context;
}

/*
 * zmq_closeContext
 * 释放zmq连接的上下文
 */
void
zmq_closeContext(void *context) {
    zmq_ctx_destroy(context);
}

/*
 * zmq_Client_setSocket
 * 建立指定套接字的客户端
 * 参数：
 * context: zmq连接上下文
 * type: 套接字类型
 * host: 服务器ip地址
 * port：服务器端口号
 */
void*
zmq_Client_setSocket(void* context, int type, string host, int port) {
    void* socket = NULL;
//    elog(INFO, "setsocket");
    string url = (string)palloc (1024);
    makeUrl (url, host, port);
//    ereport(INFO, (errmsg("%s", url)));
    if ((socket = zmq_socket(context, type))==NULL){
        elog(INFO, "socket failed");
        zmq_closeContext(context) ;
        exit(1);
    }

//    elog(INFO, "socket");
    if (zmq_connect(socket, url) < 0) {
        elog(INFO, "connect failed");
        exit(1);
    }
//    elog(INFO,"connect");
    pfree(url);
    return socket;
}

/*
 * zmq_Server_setSocket
 * 建立指定套接字的服务器端
 * 参数：
 * context: zmq连接上下文
 * type: 套接字类型
 * host: 服务器ip地址
 * port：服务器端口号
 */
void*
zmq_Server_setSocket(void* context, int type, string host, int port) {
    void* socket = zmq_socket(context, type);
    string url = (string)palloc (1024);
    makeUrl(url, host, port);
    zmq_bind(socket, url);
}

/*
 * zmq_closeSocket
 * 释放已建立的套接字连接
 * 参数：
 * socket: 套接字连接
 */
void
zmq_closeSocket(void* socket) {
    zmq_close(socket);
}

/*
 * zmq_sendStream
 * 发送数据
 * 参数：
 * socket: 套接字连接
 * s：发送消息内容
 * length: 发送消息长度
 */
int
zmq_sendStream(void* socket, string s, int length) {
    zmq_send(socket, s,length, 0 );
}

/*-------------------------------发送各类型数据----------------------------------*/
/* 参数：
 * buf: 待发送消息缓冲区
 * length: 缓冲区长度
 * msg: 待发送消息
 */
int
zmq_sendByte(string buf, int length, byte b) {
    memcpy(buf+length, &b, 1);
    length++;
    return length;
}

int
zmq_sendString(string buf,  string msg, int length) {
    memcpy(buf+length, msg, strlen(msg)+1);
    length = length+strlen(msg)+1;
    return length;
}

int
zmq_sendInt(string buf, int length, int msg) {
    memcpy(buf+length, &msg, sizeof(msg));
    length = length + sizeof(msg);
    return length;
}

int
zmq_sendFloat(string buf, int length, float msg) {
    memcpy(buf+length, &msg, sizeof(msg));
    length = length + sizeof(msg);
    return length;
}

/*
 * zmq_recvStream
 * 接收数据
 * 参数：
 * socket: 套接字连接
 * buf：接收消息内容
 */
void zmq_recvStream(void* socket, void* buf) {
    int nb = zmq_recv(socket, buf, 1024, 0);
    ((char *)buf)[nb] = '\0';
}

/*-------------------------------接收各类型数据----------------------------------*/
/* 参数：
 * buf: 已接收消息缓冲区
 * index: 目前读取位置
 */
void
zmq_recvString(void* buf , int* index, string str) {
    string s = buf;
    int tmp = *index;
    int i = strlen(s+tmp);
    memcpy(str, s+tmp, i+1);
    *index = tmp +i +1;
}

int
zmq_recvInt(void* buf, int* index) {
    int tmp = *index;
    int res = *(int*)(buf+tmp);
    *index = tmp+sizeof(res);
    return res;
}

//float
//zmq_recvFloat(void* buf, int* Index) {
//    int tmp = *index;
//    float res = *(float*)(buf+tmp);
//    index = ()tmp+sizeof(res);
//    return res;
//}

/*-------------------------------用于数据传输的Broker-Dealer套接字的构建----------------------------------*/
 string
 Client_REQ(string host, int port, string msg) {
    void *context = NULL;
    zmq_setContext(context);
    void *requester = zmq_Client_setSocket(context, ZMQ_REQ, host, port);

    zmq_sendString(requester, msg, 0 );
    string buf = (string)palloc(1024);
    zmq_recvString(buf, requester, 0);

    zmq_closeSocket(requester);
    zmq_closeContext(context);
    return buf;
 }

int
Server_DealerandRouter_broker(string host, int routerPort, int dealerPort) {
    void *context = NULL;
    zmq_setContext(context);
    void *frontend = zmq_Server_setSocket(context, ZMQ_ROUTER, host, routerPort);
    void *backend = zmq_Server_setSocket(context, ZMQ_DEALER, host, dealerPort);

    //Initialize poll set
    zmq_pollitem_t items[] = {
        {frontend, 0, ZMQ_POLLIN, 0},
        {backend, 0, ZMQ_POLLIN, 0}
    };

   // Switch messages between sockets
    while(1) {
        zmq_msg_t message;
        zmq_poll (items, 2, -1);
        if (items[0].revents & ZMQ_POLLIN )  {
            while(1) {
                //Process all parts of the message
                zmq_msg_init (&message);
                zmq_msg_recv (&message, frontend, 0);
                int more = zmq_msg_more (&message);
                zmq_msg_send (&message, backend, more? ZMQ_SNDMORE: 0);
                zmq_msg_close(&message);
                if (!more)
                    break;          // Last message part
            }
        }
        if (items[1]. revents & ZMQ_POLLIN) {
            while(1) {
                // Process all parts of the message
                zmq_msg_init(&message);
                zmq_msg_recv (&message, backend, 0);
                int more = zmq_msg_more(&message);
                zmq_msg_send(&message, frontend, more ? ZMQ_SNDMORE: 0);
                zmq_msg_close(&message);
                if (!more) {
                    break;          //Last message part
                }
            }
        }
    }

       // We never get here, but clean up anyhow
    zmq_closeSocket(frontend);
    zmq_closeSocket(backend);
    zmq_closeContext(context);
    return 0;
}

int
Server_DealerandRouter_worker(string host, int port, string (*f)(string msg)) {
    void *context = NULL;
    zmq_setContext(context);
    void *responder = zmq_Client_setSocket(context, ZMQ_REP, host, port);
    while(1) {
        // Wait for next request from client
        string s = (string) palloc (1024);
        zmq_recvString(s, responder, 0);
        printf( "Received request :[%s] \n", s );

         string rep = (*f) (s);
        free(s);
        zmq_sendString(responder, rep, 0);
    }
    zmq_closeSocket(responder);
    zmq_closeContext(context);
    return 0;
}





