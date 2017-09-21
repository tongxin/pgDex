/*
  *    MSGTYPE_H
  *    MSgType:规定dex通信过程中消息类型
  *    DType: dex通信过程中兼容的数据类型
  *　elem: dex消息包内容元素
  *     elemlist: dex消息包内容元素组
  *     消息内容获取的辅助操作
  */

#ifndef _MSGTYPE_H
#define _MSGTYPE_H

#include "common.h"
#include "network.h"

enum MsgType {
    Dex_Connect = 1,
    Dex_Disconnect = 2,
    Dex_Session = 3,
    Dex_DataFrame = 4,
    Dex_DataFrame_Partition = 5,
    Dex_DataFrame_Repartition = 6,
    Dex_DataFrame_Iterator = 7,
    Dex_Join = 8,
    Dex_Union = 9,
    Dex_Apply = 10
};
typedef enum MsgType MsgType;

enum DType {
    Dstring = 1,
    Dint = 2,
    Dstringlist = 3,
    Dintlist = 4,
    Delemlist = 5,
    Dfloat = 6
};
typedef enum DType DType;

struct elem {
    DType type;
    void* value;
};
typedef  struct elem elem;

struct elemlist{
    elem* e;
    int nums;
};
typedef struct elemlist elemlist;

string
getTypeName(DType dt);

DType
getType(string s) ;

#endif
