/*
  *   DEX_H
  *   DEX提供的API操作
  *   DEX中间件向PostgreSQL提供了一系列API接口进行DEX查询,建立与数据分析平台的连接
  */

#ifndef _DEX_H
#define _DEX_H

#include <stdlib.h>
#include <stdio.h>
#include "dexBase.h"
#include "clusterHost.h"
#include "dexConfiguration.h"

/* 创建Dex中间件
  *  Dex中间件的初始化，保存了相应的ID号
  * 数据库后台进程生成相对应的DEX中间件存储该进程的查询信息
  */
void
dex_get(Dex* dex, int backendid);

/*
  *  初始化DEX连接
  *   dex查询过程中涉及大数据分析平台的集群节点，每个查询过程存在一个状态
  *   DEX初始化阶段存储相关信息，方便信息重用
  */
int
dex_start();

 //通过dex中间件建立查询连接，为后续的查询做准备
void
dex_connect();

//指定集群屮信息建立查询连接
int
dex_connectwithCluster(Dex dex, clusterHost);

//通过中间件信息和集群节点信息建立查询连接
int
_dex_connect_cluster(Dex dex, clusterHost ch);

//断开dex查询连接
void
dex_disconnect(const Dex *dex);

//int
//dex_createSession(Dex dex);
//
//int
//dex_createSessionWithPath(Dex dex,  string clusterPath);
//
//int
//_dex_createSession(Dex dex, string clusterPath);


int
dex_createDataFrameFromTable(
    const Dex *dex, 
    string dfname,
    string dbname,
    string tbname,
    int partitions
);    //创建数据集

// 指定数据表中的多列作为数据集
int
dex_createDataFrameFromTableProjection(
    const Dex *dex,
    string dfname, 
    string dbname,
    string tablename,
    int partitions,
    cstrlist *proj
);

//数据集的join操作
int
dex_joinDataFrame(Dex *dex, string newDF, string df1, string df2, strlist* collist, elemlist *elist) ;

//数据集的union操作
int
dex_unionDataFrame(Dex *dex,string newDF,  string df1, string df2);

//使用已有的数据分析方法对数据集进行数据分析
int
dex_applyFunctions(Dex* dex, string tableName, string funcName, strlist* paramslist);
#endif // _DEX_H
