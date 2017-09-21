/*
  *   DEXDATAFRAME_H
  *    基于基本数据单位进行数据分析
  *    dexDataFrame: DEX进行数据分析的基本操作单位
  */

#ifndef _DEXDATAFRAME_H
#define _DEXDATAFRAME_H

#include "dexBase.h"

#define DATAFRAME_EXISTS 1
#define DATAFRAME_NOT_EXISTS -1

struct dexDataFrame
{
    dexTupledesc* schema;
    int partitionNum;
};
typedef struct dexDataFrame dexDataFrame;

//创建数据集，为后续操作提供操作对象
int
dexDataFrame_create(Dex *dex,string dataframe, string databaseName, string tablename, int numPartitions, cstrlist *projection) ;

//存储数据对象的schema，方便用户查看
int
dexDataFrame_recordschema(string dataframe, dexDataFrame ddf, int backendId) ;

//存储数据集的分区情况
int
dexDataFrame_recordPartitions(string dataframe, dexDataFrame ddf, int backendId);

//当查询过程终止时，清空该过程产生的数据对象
int
dexDataFrame_drop(int backendID) ;

//在dexDataFrame数据单位上进行join操作
int
dexDataFrame_join(Dex *dex, string newdataframe,  string dataframe1, string dataframe2, strlist* cols, elemlist *elist);

//在dexDataFrame数据单位上进行union操作
int
dexDataFrame_union(Dex *dex, string newdataframe, string dataframe1, string dataframe2);

//在dexDataFrame数据单位上进行数据分析操作
int
dexDataFrame_apply (Dex *dex, string dataframe, string funcName, strlist* params);


#endif // _DEXDATAFRAME_H

