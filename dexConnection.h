/*
  *  dexConnection_H
  *   DEX查询过程连接机制
  *   dexConnection: DEX连接的基本数据结构
  */
#ifndef _DEXCONNECTION_H
#define _DEXCONNECTION_H

#define DISCONNECT 1
#define DISCONNECT_ERROR -1

#include "dexBase.h"
#include "common.h"
#include "clusterHost.h"

struct dexConnection{
    elem master;
};
typedef struct dexConnection dexConnection;

//DEX查询建立连接API
Dex
dexConnection_connect(clusterHost ch) ;

//DEX查询断开连接API
void
dexConnection_disconnect(Dex *dex);

#endif // _DEXCONNECTION_H
