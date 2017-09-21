/*
  *  DEX_C
  *  dex查询过程API
  */

#include "dex.h"
#include "dexConnection.h"
#include "dexDataFrame.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/geo_decls.h"

/*
  *  dex_start
  *  dex查询过程初始化
  */
int
dex_start()
{
    load_cluster_config();
    create_dexconfig_table();
    create_dfmodel_table();
    create_dfpartition_table();
    return 1;
}

/*
  *  dex_get
  *  当dex中间件不存在时，创建新的dex中间件;
  　当该后台进程对应的dex中间件存在时，获取已有的dex中间件
  *  参数：
  *  dex:  获取到的dex存储地址
  *  backendid: 后台进程　
  */
void
dex_get(Dex* dex, int backendid)
{
    elog(INFO, "dex_get");
    dexConfiguration_get(dex, backendid);
}

/*
  *  dex_connect
  *  dex连接ＡＰＩ
  */
void
dex_connect()
{
    elog(INFO, "dex_connect");

    Dex dex = dexConnection_connect(clusterHost_get());
 
    dexConfiguration_set(&dex);
}

// /*
//   *  dex_connect
//   *  dex连接ＡＰＩ，指定要连接的集群节点host
//   *  参数：
//   *  dex:  用于连接的dex信息
//   *  ch: 指定的集群节点host
//   */
// int
// dex_connectwithCluster(Dex dex, clusterHost ch)
// {
//     if (clusterHost_validate(ch) >0 )
//     {
//         return _dex_connect_cluster(dex, ch);
//     }
//     else
//     {
//         elog(ERROR,  "Error clusterHost or The clusterHost is occupied");
//         return -1;
//     }
// }

/*
  *  _dex_connect_cluster
  *  dex连接实现
  *  参数：
  *  dex:  用于连接的dex信息
  *  ch: 指定的集群节点host
  */
// int
// _dex_connect_cluster(Dex dex, clusterHost ch)
// {
//     elog(INFO, "dex connect cluster");
//     dex = dexConnection_connect(dex, ch);
//     elog(INFO,"dexConfiguration");
//     dexConfiguration_set(dex);
//     elog(INFO, "connect");
//     return 1;
// }

/*
  *  dex_disconnect
  *  dex断开连接ＡＰＩ
  *  参数：
  *  dex:  该连接查询中的dex信息
  */
void
dex_disconnect(const Dex *dex)
{
    elog(INFO, "dex_disconnect");
    dexConnection_disconnect(dex);
    elog(INFO, "setConnState");
    dexConfiguration_setConnState(dex, DEXUNCONNECTED);
}

//int
//dex_createSession(Dex dex)
//{
//    return _dex_createSession(dex, NULL);
//}
//
//int
//dex_createSessionWithPath(Dex dex,  string clusterPath)
//{
//    return _dex_createSession(dex, clusterPath);
//}
//
//int
//_dex_createSession(Dex dex, string clusterPath)
//{
//    return dexSession_create(dex, clusterPath);
//}

/*
  *  dex_createDataFrame
  *  数据集创建API
  *  参数：
  *  dex:  连接查询中的dex信息
  *  df :  数据集名称
  *  databaseName: 源数据所在的数据库
  *  tableName:  源数据所在的表格
  */
int
dex_createDataFrameFromTable(
    const Dex *dex, 
    string dfname, 
    string dbname,
    string tbname,
    int partitions
)
{
    return dexDataFrame_create(dex, dfname, dbname, tbname, partitions, NULL);
}

/*
  *  dex_createDataframeFromTableProjection
  *  数据集创建API，指定表格中的某几列作为源数据
  *  参数：
  *  dex:  连接查询中的dex信息
  *  df :  数据集名称
  *  databaseName: 源数据所在的数据库
  *  tableName:  源数据所在的表格
  *  colnames: 指定列名
  */
int
dex_createDataframeFromTableProjection(
    const Dex *dex, 
    string dfname, 
    string dbname,
    string tbname,
    int partitions,
    cstrlist *proj
)
{
    return dexDataFrame_create(dex, dfname, dbname, tbname, partitions, proj);
}


/*
  *  dex_joinDataFrame
  *  dexDataFrame的join操作api
  *  参数：
  *  dex:  连接查询中的dex信息
  *  newDF: 新的数据集名称
  *  df1: 用于进行join操作的数据集1
  *  df2: 用于进行join操作的数据集２
  *  collist: 作为约束条件的列属性
  *  elist: 返回join操作后的列属性
  *  返回值：0:创建失败，1：创建成功
  */
int
dex_joinDataFrame(Dex *dex, string newDF, string df1, string df2, strlist* collist, elemlist *elist)
{
    return dexDataFrame_join(dex, newDF, df1,df2, collist, elist);
}

/*
  *  dex_unionDataFrame
  *  dexDataFrame的union操作api
  *  参数：
  *  dex:  连接查询中的dex信息
  *  newDF: 新的数据集名称
  *  df1: 用于进行join操作的数据集1
  *  df2: 用于进行join操作的数据集２
  *  返回值：0:创建失败，1：创建成功
  */
int
dex_unionDataFrame(Dex* dex,string newDF,  string df1, string df2)
{
    return dexDataFrame_union(dex, newDF,df1,df2);
}

/*
  *  dex_applyFunctions
  *  dexDataFrame的数据分析操作api
  *  参数：
  *  dex:  连接查询中的dex信息
  *  df: 新的数据集名称
  *  funcName: 数据分析操作函数名
  *  paramslist: 数据分析操作函数参数
  */
int
dex_applyFunctions(Dex* dex, string df, string funcName, strlist* paramslist){
    return dexDataFrame_apply(dex, df, funcName, paramslist);
}
