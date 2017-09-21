/*
  * DEXPROTOCOL_H
  * DEX通信过程中的消息格式，消息类型，编码格式
  * dexConnMsg: 连接消息
  * dexDisConnMsg: 断开连接消息
  * dexDataFrameMsg: 数据集创建消息
  * dexDataFramePartitionMsg: 数据集分区信息
  * dexDataFrameRepartitionMsg: 数据集重分区
  * dexDataFrameIteraterMsg: 数据集元素信息
  * dexJoinMsg:数据集进行join操作
  * dexUnionMsg: 数据集进行union操作
  * dexApplyMsg: 数据集进行相应的数据分析操作
  */
#ifndef _DEXPROTOCOL_H
#define _DEXPROTOCOL_H

#include "MsgType.h"
#include "network.h"
#include "dexProtocol.h"
#include "dexBase.h"

struct dexConnMsg {
    elem* master;
};
typedef struct dexConnMsg dexConnMsg;

dexConnMsg *
dexConnMsg_build (int type, char *hostname);

struct dexDisConnMsg {
    elem* master;
};
typedef struct dexDisConnMsg dexDisConnMsg;

dexDisConnMsg *
dexDisConnMsg_build (char *hostname);

//struct dexSessionMsg {
//    elem* clusterPath;
//};
//typedef struct dexSessionMsg dexSessionMsg;

struct dexDataFrameMsg {
    elem* dataframe;
    elem* tablename;
    elem* numPartitions;
    elem* db_properties;
};
typedef struct dexDataFrameMsg dexDataFrameMsg;

struct dexDataFramePartitionMsg {
    elem* dataframe;
    elem* partitionId;
};
typedef struct dexDataFramePartitionMsg dexDataFramePartitionMsg;

struct dexDataFrameRepartitionMsg {
    elem* newDataframe ;
    elem* oldDataframe;
    elem* numsPartition;
    elem* partitionMethod;
};
typedef struct dexDataFrameRepartitionMsg dexDataFrameRepartitionMsg;

struct dexDataFrameIterMsg{
    elem* dataframeIter;
    elem* dataframe;
    elem* partitionId;
    elem* iterId;
};
typedef struct dexDataFrameIterMsg dexDataFrameIterMsg;

struct dexJoinMsg {
    elem* newDataframe;
    elem* dataframe1;
    elem* dataframe2;
    elem* cols;
};
typedef struct dexJoinMsg dexJoinMsg;

struct dexUnionMsg {
    elem* newDataframe;
    elem* dataframe1;
    elem* dataframe2;
};
typedef struct dexUnionMsg dexUnionMsg;

struct dexApplyMsg {
    elem* dataframe;
    elem* funcName;
    elem* params;
};
typedef struct dexApplyMsg dexApplyMsg;

//消息初始化
int
Dex_Connect_Msg_Init(dexConnMsg* dcm,string master);

int
Dex_Disonnect_Msg_Init(dexDisConnMsg* ddm, string master) ;

//int
// Dex_Session_Msg_Init(dexSessionMsg* dsm, string clusterPath);

int
 Dex_DataFrame_Msg_Init(dexDataFrameMsg* ddfm, string dataframe, string tablename, int* numPartitions, DBConn_properties db) ;

int
Dex_DataFrame_Partition_Msg_Init(dexDataFramePartitionMsg*ddfpm, string dataframe, int partitionId) ;

int
Dex_DataFrame_Repartition_Msg_Init(dexDataFrameRepartitionMsg* ddfrm, string newDataframe, string oldDataframe,  int numsPartition, string partitionMethod) ;

int
Dex_DataFrame_Iterator_Msg_Init(dexDataFrameIterMsg* ddfim, string iterator, string dataframe, int partitionId) ;

int
Dex_Join_Msg_Init(dexJoinMsg* djm, string newDataframe, string dataframe1, string dataframe2, strlist* cols) ;

int
Dex_Union_Msg_Init(dexUnionMsg* dum, string newDataframe, string dataframe1, string dataframe2);

int
Dex_Apply_Msg_Init(dexApplyMsg* dam, string dataframe, string funcname, strlist* params) ;

//DEX通信过程中读取和发送数据
void
writeMessage(string host, int port, MsgType mt, void* msg, void* Recvbuf) ;

int
writeMessageBegin(string buf, int length, MsgType mt);

int
writeMessageEnd(void* requester, string buf, int length)  ;

int
writeField(string buf, int length , void* msg, MsgType mt);

void
readString(void* buf, int* index,  string s);

int
readInt(void* buf, int* index);


#endif // _DEXPROTOCOL_H
