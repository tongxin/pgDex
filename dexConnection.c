/*
 * DEXCONNECTION_C
 * dex连接过程中的api
 */
#include "dexConnection.h"
#include "dexProtocol.h"
#include "postgres.h"

/*
  *  dexConnection_connect
  *  dex建立连接
  *   参数：
  *  dex: dex中间件的基本信息
  *  ch: 待连接的集群节点host
  */
Dex
dexConnection_connect (clusterHost ch) {
    Dex dex;
    char s[256];

    ereport(INFO, (errmsg("%s, %d", ch.hostname, ch.port)));

    dexConnMsg* dcm = dexConnMsg_build(1, ch.hostname);
    writeMessage(ch.hostname, ch.port, Dex_Connect, dcm, &s);
    ereport(INFO, (errmsg("%s",ch.hostname)));
    string* arr = string_split(&s, ':');
    dex.host = arr[0];
    dex.port = stringToInt(arr[1]);
    dex.state = 1;

    free(arr);
    ereport(INFO, (errmsg("%s",dex.host)));
    elog(INFO, "connect end");
    return dex;
}

/*
  *  dexConfiguration_disconnect
  *  dex 断开连接
  *   参数：
  *  dex: dex中间件的基本信息
  */
void
dexConnection_disconnect(Dex *dex) {
    dexDisConnMsg *ddcm = dexDisConnMsg_build(dex->host);
    char flag[16] = "\0"; 
    ereport(INFO, (errmsg("%s,%d", dex->host, dex->port)));
    writeMessage(dex->host, dex->port, Dex_Disconnect, (void*)ddcm, &flag);
//    if (string_isequal(flag, "1")) {
//        return DISCONNECT;
//    } else {
//        return DISCONNECT_ERROR;
//    }
    elog(INFO, "disconnect end");
}
