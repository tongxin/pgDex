/*
 * DEXSESSION_C（已省去会话这一过程）
 * dex查询会话接口
 */
#include "dexSession.h"
#include "dexProtocol.h"

/*
 * dexSession_create
 * dex查询创建会话过程，重用会话已有的变量信息
 * 参数：
 * dex: dex连接信息
 * clusterPath: 集群节点ip地址
 */
//int
//dexSession_create(Dex dex, string clusterPath) {
//    dexSessionMsg* ddm;
//    Dex_Session_Msg_Init(ddm, clusterPath);
//    string s = (string)palloc(sizeof(char)*10);
//    writeMessage(dex.host, dex.port, Dex_Disconnect, ddm,s);
//    char flag = s[0];
//    pfree(s);
//    if (flag) {
//        return SESSION_CREATE;
//    } else {
//        return SESSION_ERROR;
//    }
//}

