/*
  *  CLUSTERHOST_H
  *  对集群信息进行管理　
  *  用户通过指定可用的集群节点信息来完成DEX查询连接
  */

#ifndef _CLUSTERHOST_H
#define _CLUSTERHOST_H

#include "common.h"

struct clusterHost {
    char hostname[1024];
    int port;
};
typedef struct clusterHost clusterHost;

void
load_cluster_config();         //创建集群信息表，管理集群信息

clusterHost
clusterHost_get();        //获取闲置的cluster信息

int
clusterHost_validate(clusterHost *); //验证用户指定的clusterHost是否存在或是否空闲

#endif // _CLUSTERHOST_H
