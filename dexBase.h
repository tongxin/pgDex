/*
  *  DEXBASE_H
  *   Dex查询中的基本数据结构
  *   Dex: Dex的基本数据结构，存储DEX的基本信息
  *   DBConn_properties：　建立Dex查询连接的信息
  *   dexTupledesc: Dex查询过程消息内容的属性
  *   dexTuple: Dex查询过程消息内容
  */

#ifndef _DEXBASE_H
#define _DEXBASE_H

#define DEXCONNECTED 1
#define DEXUNCONNECTED -1

#include "common.h"
#include "MsgType.h"

struct Dex {
string host;
int port;
int state;
int backendid;
};
typedef struct Dex Dex;

struct DBConn_properties{
    string url ;
    string  user;
    string passwd;
    string driver;
};
typedef struct DBConn_properties DBConn_properties ;

typedef elemlist dexTupledesc;
typedef elemlist dexTuple;


#endif // _DEXBASE_H

