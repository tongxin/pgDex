/*
 * DEXPROTOCOL_C
 * dex通信过程所需的消息类型，消息格式和消息传输过程设计
 */
#include "dexProtocol.h"
#include "postgres.h"
#include "fmgr.h"
#include <zmq.h>

/*----------------------------------各类消息类型的初始化---------------------------------*/
int
Dex_Connect_Msg_Init(dexConnMsg* dcm, string master)
{
    dcm->master->type = 1;
    dcm->master->value = master;
    return 1;
}

dexConnMsg *
dexConnMsg_build (int type, char *hostname) {
    dexConnMsg *msg = (dexConnMsg*) palloc(sizeof(dexConnMsg));
    elem *e = msg->master = (elem*)palloc(sizeof(elem));
    e->type = type;
    e->value = string_dup(hostname);
}


int
Dex_Disonnect_Msg_Init(dexDisConnMsg* ddm, string master)
{
    ddm->master->type = 1;
//    ereport(INFO, (errmsg("%s", master)));
    ddm->master ->value = master;
    return 1;
}

dexDisConnMsg *
dexDisConnMsg_build (char *hostname) {
    dexDisConnMsg *msg = (dexDisConnMsg*) palloc(sizeof(dexDisConnMsg));
    elem *e = msg->master = (elem*)palloc(sizeof(elem));
    e->type = 1;
    e->value = hostname;
}

//int
//Dex_Session_Msg_Init(dexSessionMsg* dsm, string clusterPath)
//{
//    dexSessionMsg dsm;
//    dsm->clusterPath->type = 1;
//    dsm->clusterPath->value = clusterPath;
//    return dsm ;
//    return 1;
//}

dexDataFrameMsg*
dexDataFrameMsg_build(
    string dfname, 
    string tbname, 
    int partition,
    cstrlist *projection,
    DBConn_properties db
)
{
    dexDataFrameMsg *msg = (dexDataFrameMsg *) palloc(sizeof(dexDataFrameMsg));
    msg->dataframe = (elem *)palloc(sizeof(elem));
    msg->tablename = (elem *)palloc(sizeof(elem));
    msg->numPartitions = (elem *)palloc(sizeof(elem));
    msg->db_properties = (elem *)palloc(sizeof(elem));
    
    msg->dataframe->type = 1;
    msg->dataframe->value = dfname;
    msg->tablename->type = 1;
    msg->tablename->value = tbname;
    msg->numPartitions->type = 2;
    msg->numPartitions->value = partition;
//    ereport(INFO, (errmsg("numPartitions: %d", &numPartitions)));
    msg->db_properties->type = 3;
    strlist* sl = strlist_new(4);
    strlist_add(sl, db.url);
    strlist_add(sl, db.user);
    strlist_add(sl, db.passwd);
    strlist_add(sl, db.driver);
    msg->db_properties->value = sl;
    return msg;
}

int
Dex_DataFrame_Partition_Msg_Init(dexDataFramePartitionMsg*ddfpm, string dataframe, int partitionId)
{
//    dexDataFramePartitionMsg ddfpm;
    ddfpm->dataframe->type = 1;
    ddfpm->dataframe->value = dataframe;
    ddfpm->partitionId->type = 2;
    ddfpm->partitionId->value = partitionId;
//    return ddfpm;
    return 1;
}

int
Dex_DataFrame_Repartition_Msg_Init(dexDataFrameRepartitionMsg* ddfrm, string newDataframe, string oldDataframe,  int numsPartition, string partitionMethod)
{
    ddfrm->newDataframe->type = 1;
    ddfrm->newDataframe ->value = newDataframe;
    ddfrm->oldDataframe ->type = 1;
    ddfrm->oldDataframe ->value = oldDataframe;
    ddfrm->numsPartition->type = 2;
    ddfrm->numsPartition->value = numsPartition;
    ddfrm->partitionMethod->type = 1;
    ddfrm->partitionMethod->value = partitionMethod;
//    return ddfrm;
    return 1;
}

int
Dex_DataFrame_Iterator_Msg_Init(dexDataFrameIterMsg* ddfim, string iterator, string dataframe, int partitionId)
{
//    dexDataFrameIterMsg ddfim;
    ddfim->dataframeIter->type=1;
    ddfim->dataframeIter->value = iterator;
    ddfim->dataframe ->type = 1;
    ddfim->dataframe->value = dataframe;
    ddfim->partitionId ->type = 2;
    ddfim->partitionId->value = partitionId;
//    return ddfim;
    return 0;
}

int
Dex_Join_Msg_Init(dexJoinMsg* djm, string newDataframe, string dataframe1, string dataframe2, strlist* cols)
{
    djm->newDataframe->type = 1;
    djm->newDataframe->value = newDataframe;
    djm->dataframe1->type = 1;
    djm->dataframe1 ->value  = dataframe1;
    djm->dataframe2 ->type = 1;
    djm->dataframe2 ->value = dataframe2;
    djm->cols->type = 3;
    djm->cols->value = cols;
    return 1;
}

int
Dex_Union_Msg_Init(dexUnionMsg* dum, string newDataframe, string dataframe1, string dataframe2)
{
    dum->newDataframe->type = 1;
    dum->newDataframe->value = newDataframe;
    dum->dataframe1 ->type = 1;
    dum->dataframe1 ->value = dataframe1;
    dum->dataframe2->type = 1;
    dum->dataframe2->value = dataframe2;
    return 1;
}

int
Dex_Apply_Msg_Init(dexApplyMsg* dam,  string dataframe, string funcname, strlist* params)
{
    dam->dataframe->type = 1;
    dam->dataframe->value = dataframe;
    dam->funcName->type = 1;
    dam->funcName->value = funcname;
    dam->params->type = 3;
    dam->params->value = params;
    return 1;
}


/*----------------------------------消息传输接口---------------------------------*/

/*
 * writeMessage
 * 发送请求消息
 * 参数：
 * host: 服务器目标节点host
 * port：服务器目标端口port
 * mt: 消息类型
 * msg: 消息内容
 * RecvBuf: 返回响应消息内容
 */
void
writeMessage(string host, int port, MsgType mt, void* msg, void* RecvBuf)
{
    elog(INFO, "writeMessage");
    void* context = NULL;
    context = zmq_setContext();
    void* socket = NULL;
    socket = zmq_Client_setSocket(context, ZMQ_REQ, host, port);
    string buf = (string)palloc(1024);
    int length = 0;
    length = writeMessageBegin(buf, length, mt) ;
    length = writeField(buf,length, msg, mt);
//    ereport(INFO, (errmsg("%d", length)));
    length = writeMessageEnd(socket, buf,length);
    pfree(buf);
    elog(INFO, "send_end");
    //接收响应消息
//    elog(INFO, "zmq recieve string");
    zmq_recvStream(socket, RecvBuf);
    ereport(INFO, (errmsg("recvString:%s", RecvBuf)));
    elog(INFO, "recv_end");

    zmq_closeSocket(socket);
    zmq_closeContext(context);

//    elog(INFO, "socket close");
}

/*
 * writeMessageBegin
 * 开始发送请求消息，确定消息长度
 * 参数：
 * buf: 待发送的消息
 * length: 消息内容长度
 * mt: 消息类型
 */
int
writeMessageBegin(string buf, int length, MsgType mt)
{
//    elog(INFO, "writeMessageBegin");
    length = writeInt(buf, length, mt);
    return length;
}


/*
 * writeMessageEnd
 * 发送消息
 * 参数：
 * requester: zeromq提供的REQ-REP套接口的客户端
 * buf: 待发送的消息
 * length: 消息内容长度
 */
int
writeMessageEnd(void* requester, string buf, int length)
{
//    elog(INFO, "writeMessageEnd");
    zmq_sendStream(requester,buf, length);
    return length;
}

/*
 * writeMessageField
 * 发送消息
 * 参数：
 * buf: 待发送的消息
 * length: 消息内容长度
 * msg：消息内容
 * mt: 消息类型
 */
int
writeField(string buf, int length , void* msg, MsgType mt)
{
    elem** e = msg;
//    elog(INFO, "writeField");
    int num  = getSize(mt);
    int i = 0;
    for(; i < num; i++)
    {
        length = writeElem(buf, length, *(*e++));
    }
    return length;
}

/*
 * writeElem
 * 发送消息内的各类型消息
 * 参数：
 * buf: 待发送的消息
 * length: 消息内容长度
 * e:某一类型消息
 */
int
writeElem(string buf, int length, elem e)
{
//    elog(INFO, "writeElem");
    int flag = 1;
//    ereport(INFO, (errmsg("%d",e.type)));
    switch (e.type)
    {
    case Dstring :
//            writeString(requester, (string)e.value, more);
        length = writeString(buf, length, (string)e.value);
//        ereport(INFO, (errmsg("%s",e.value)));
        break;
    case Dint:
//            writeInt(requester, *(int*)e.value, more);
        length = writeInt(buf, length,*(int*)e.value);
        break;
    case Dstringlist:
//            writeStringList(requester, (string*)e.value, more);
        length = writeStringList(buf, length,(strlist*)e.value);
        break;
    case Dintlist:
//            writeIntList(requester, (int*)e.value, more);
        length = writeIntList(buf, length,(int*)e.value);
        break;
//        case Dfloat:
////            writeFloat(requester, *(float*)e.value, more);
//            length = writeFloat(buf, length,*(float*)e.value);
//            break;
    case Delemlist:
//            writeElemList(requester, e.value);
        length = writeElemList(buf, length,e.value);
        break;
    default:
        printf("wrong type");
        flag = -1;
        break;
    }
    return length;
}

/*----------------------------------各数据类型的消息发送---------------------------------*/
int
writeInt(string buf, int length, int msg)
{
//    elog(INFO, "writeInt");
    length = zmq_sendInt(buf, length, msg);
    return length;
}

int
writeString(string buf, int length, string msg)
{
    length = zmq_sendString(buf, length, msg);
    return length;
}

int
writeFloat(string buf, int length, float msg)
{
//    length = zmq_sendFloat(buf, length, msg);
    return length;
}

int
writeStringList(string buf, int length, strlist* msg)
{
//    elog(INFO, "writeStringList");
    int len = msg->count;
    length = zmq_sendByte(buf, length, (byte)len);
    int i = 0;
//    ereport(INFO, (errmsg("lenght of strlist: %d", len )));
    for(; i < len; i++)
    {
        length = zmq_sendString(buf, length,msg->list[i]);
        ereport(INFO, (errmsg("%s", msg->list[i])));
    }
    return length;
}

int
writeIntList(string buf, int length, int* msg)
{
    int len = strlen(msg);
    length = zmq_sendByte(buf, length, (byte)len);
    int i = 0;
    for(; i < length - 1; i++)
    {
        length = zmq_sendInt(buf, length, msg[i]);
    }
    length= zmq_sendInt(buf, length, msg[i]);
    return length;
}

int
writeElemList(string buf, int length,elemlist elist)
{
    int i = 0;
    elem* el = elist.e;
    for (; i < elist.nums; i ++)
    {
        length = writeElem(buf, length,el[i]);
    }
    return length;
}

/*----------------------------------各数据类型的消息接收---------------------------------*/
void
readString(void* buf, int* index,  string s)
{
    zmq_recvString(buf, index, s);
}

int
readInt(void* buf, int* index)
{
    zmq_recvInt(buf, index);
}


