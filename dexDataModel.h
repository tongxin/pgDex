/*
  * DEXDATAMODEL_H
  *  与数据库进行交互
  *  对数据表的管理
  */

#ifndef  _DEXDATAMODEL_H
#define _DEXDATAMODEL_H

#include "MsgType.h"
#include "dexBase.h"

//DEX据单位属性
int
dex_get_model(dexTupledesc* dtd, string sql ) ;

//DEX数据单位内容
int
dex_get_data(int partitionID, string data);

//DEX数据单位分区情况
int
dex_partition_data(int numsPartition);

//数据表插入操作
int
dex_insert_table(string table, int num,  dexTuple* dtuples) ;

int
dex_insert_table_sql(string sql) ;

//数据表创建操作
int
dex_create_table(string table, dexTupledesc dtdesc) ;

int
dex_create_table_sql (string sql);

//数据表删除操作
int
dex_drop_table(string table);

int
dex_drop_table_sql(string sql) ;

//数据表查询
int
dex_select_table(string table, strlist* sl) ;

int
dex_select_table_sql(string sql);

int
dex_select_tuple(dexTuple* dt, string sql);

int
dex_select_tuples(dexTuple *dt, string sql);

//数据表更新操作
int
dex_update_table_sql(string sql);

#endif // _DEXDATAMODEL_H

