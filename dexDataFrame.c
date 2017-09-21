/*
  * DEXDATAFRAME_C
  * DEX查询过程中数据集操作api
  */
#include "dexDataFrame.h"
#include "dexProtocol.h"
#include "dexDataModel.h"
#include "dexIterator.h"

#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "executor/executor.h"

/*
  *  dex_setDBConnProperties
  *  根据配置文件，配置数据集连接过程的连接参数
  *   参数：
  *  db: 存储连接信息
  *  fileName: 数据库的连接配置文件
  *  databaseName: 数据库文件名
  */
void
dex_setDBConnProperties(DBConn_properties* db, string fileName, string databaseName)
{
    elog(INFO, "set DBConnProperties");
    FILE *fp;
    ereport(INFO, (errmsg("%s",fileName)));
    if ((fp = fopen(fileName, "r")) == NULL)
    {
        elog(INFO,"fail to read");
        exit(1);
    }
    while(true)
    {
        string tmp = (string) malloc(64);
        if (fgets(tmp, 64, fp) != NULL)
        {
            tmp[strlen(tmp)-1] = '\0';
            string* arr  = string_split(tmp, '=');
            if (string_isequal(arr[0], "url"))
            {
                db->url = (string) palloc(sizeof(char)*128);
                string_init(db->url, arr[1]);
                string_contat(db->url, 2 ,"/", databaseName);
            }
            else if( string_isequal(arr[0], "user"))
            {
                db->user = (string) palloc(sizeof(char)*128);
                string_init(db->user, arr[1]);
            }
            else if(string_isequal(arr[0], "password"))
            {
                db->passwd = (string) palloc(sizeof(char)*128);
                string_init(db->passwd, arr[1]);
            }
            else if(string_isequal(arr[0], "driver"))
            {
                db->driver = (string) palloc(sizeof(char)*128);
                string_init(db->driver, arr[1]);
            }
        }
        else
            break;
    }
    fclose(fp);
    fp = NULL;
    ereport(INFO,(errmsg("%s,%s, %s, %s",db->driver,db->passwd,db->url,db->user)));
}

/*
  *  dexDataFrame_DBConn
  *  初始化连接信息结构体
  *   参数：
  *  url：数据库所在节点host, port
  *  user: 数据库权限用户
  *  password: 数据库权限用户登陆密码
  * driver: 连接驱动
  */
DBConn_properties
dexDataFrame_DBConn (string url , string user, string password, string driver)
{
    DBConn_properties db;
    db.url = url;
    db.user = user;
    db.passwd = password;
    db.driver =  driver;
    return db;
}

/*
  *  dexDataFrame_create
  *  创建数据集api
  *   参数：
  *  dex: dex中间件信息结构体
  *  dataframe: 数据集名称
  *  databaseName: 数据库名
  *  tablename: 数据表名
  *  numPartitions: 分区数
  */
int
dexDataFrame_create(
    Dex *dex,
    string dfname, 
    string dbname, 
    string tablename, 
    int numPartitions,
    cstrlist *projection
)
{
    elog(INFO, "dexDataFrame_create");

    char buffer[1024];

    int flag = 0;
    if (dexDataFrame_validate(dfname) == DATAFRAME_EXISTS) {
        ereport(ERROR, (errmsg("Dataframe %s already exists.", dfname)));
        return 0;
    }

    //在spark上生成相应的dataframe
    getcwd(&buffer, 256);
    string_contat(&buffer, 1, "/pg.ini");
    DBConn_properties *dbConn = (DBConn_properties *)palloc(sizeof(DBConn_properties));
    dex_setDBConnProperties(dbConn, &buffer, dbname);
    
    //        ereport(INFO, (errmsg("%s, %s, %s, %s", dbConn->driver, dbConn->passwd,dbConn->url, dbConn->user)));
    dexDataFrameMsg *msg = dexDataFrameMsg_build(dfname, tablename, numPartitions, projection, *dbConn);
    //        elog(INFO, "Dex_DataFrame_Msg");
    //        ereport(INFO, (errmsg("%d", *(int*)ddfm->numPartitions->value)));
    //        ereport(INFO, (errmsg("%d", *(int*)ddfm->numPartitions->value)));
    writeMessage(dex->host, dex->port, Dex_DataFrame, msg, &buffer);
    flag = stringToInt(&buffer);
    //        ereport(INFO, (errmsg("%d",flag)));
    if (flag <= 0) 
        return flag;
    //spark  成功生成相应的dataframe;
    //存储一个session中生成的 dataframe;
    dexDataFrame ddf;
    ddf.schema = (dexTupledesc *)palloc(sizeof(dexTupledesc));
    ddf.partitionNum = numPartitions;

    string_init(&buffer, "SELECT * FROM ");
    string_contat(&buffer, 1, tablename);    
    dex_get_schema(ddf.schema, &buffer);

    dexDataFrame_recordSchema(dfname, ddf, dex->backendid); //存储dataframe 的标识符， 以及 对应的 dataschema
    dexDataFrame_recordPartitions(dfname, ddf, dex->backendid);

    return flag;
}

/*
  *  dexDataFrame_getDataschema
  *  已知数据集的源数据表，获取现有数据集的schema
  *   参数：
  *  td: 存储dataframe的列属性及列类型
  *  tablename: 数据表名
  */
int
dexDataFrame_getDataschema(dexTupledesc *td, string tablename)
{
    string sql = (string) palloc(sizeof(char)*1024);
    string_init(sql, "SELECT * FROM ");
    string_contat(sql, 1, tablename);
//    ereport(INFO, (errmsg("%s",sql)));
    return dex_get_schema(td, sql );
}

/*
  *  dexDataFrame_getType
  *  获取schema中指定列的数据类型
  *   参数：
  *  td: dataframe 的schema信息
  *  i : 指定列id
  */
string
dexDataFrame_getType(dexTupledesc *td, int i )
{
    elem* e = td->e;
    return getTypeName(e[i].type);
}

/*
  *  dexDataFrame_getfname
  *  获取schema中指定列的名称
  *   参数：
  *  td: dataframe 的schema信息
  *  i : 指定列id
  */
string
dexDataFrame_getfname(dexTupledesc *td, int i )
{
    elem* e = td->e;
    return e[i].value;
}

/*
  *  dexDataFrame_getfnameId
  *  根据列名获取schema中指定列序号
  *   参数：
  *  td: dataframe 的schema信息
  *  name: 列名
  */
int
dexDataFrame_getfnameId(dexTupledesc *td, string name)
{
    int i = 0;
    elem* e  = td -> e;
    for(; i < td->nums; i++ )
    {
        if(string_isequal(e[i].value, name))
        {
            break ;
        }
    }
    if (i == td->nums)
        return -1;
    else
        return i;
}


/*
  *  dexDataFrame_getNumPartitons
  *  获取指定dataframe的分区情况
  *   参数：
  *  dataframe: 指定数据集名称
  *  dex: dex中间件的基本信息
  */
int
dexDataFrame_getNumPartitions(string dataframe, Dex dex)
{
    return dexDataFrame_getNumPartitionsFromTable(dataframe, dex.backendid);
}

/*
  *  dexDataFrame_repartition
  *  dataframe重分区
  *   参数：
  *  dex: dex配置信息
  *  newdataframe: 生成的新的数据集
  *  olddataframe: 待重分区的数据集
  *  numsPartitionID: 重分区数
  *  partitionMethod: 分区方式
  */
int
dexDataFrame_repartition(Dex dex, string newdataframe, string olddataframe, int numsPartitionID, string partitionMethod)
{
    dexDataFrameRepartitionMsg*  ddfrm;
    Dex_DataFrame_Repartition_Msg_Init(ddfrm, newdataframe, olddataframe, numsPartitionID, partitionMethod);
    string s = (string)palloc(sizeof(char)*10);
    writeMessage(dex.host, dex.port, Dex_DataFrame_Repartition, ddfrm,s);
    int flag = stringToInt(s);
    pfree(s);
    return flag;
}

//iterator
//存储于spark
//获取每一行数据
//string
//dexDataFrame_getIterator(Dex dex, string iterator, string dataframe) {
//    return dexIterator_create(iterator,dataframe, dex);
//}

/*
  *  dexDataFrame_createTable
  *  创建数据表存储dex查询过程中存在的dataframe信息
  */
int
create_dfmodel_table() {
    char SQL[] = "CREATE TABLE IF NOT EXISTS dataFrameModel(dataframe text, colname text, coltype int, backendID int)";
    int rs = execStatement(&SQL);

    return rs;
}

int
create_dfpartition_table() {
    char SQL[] = "CREATE TABLE IF NOT EXISTS dataFramePartitions(dataframe text, numPartitions int, backendID int)";
    int rs = execStatement(&SQL);
    return rs;
}

/*
  *  dexDataFrame_recordschema
  *  记录dex查询过程中生成的dataframe的schema
  *  参数：
  *  dataframe: 数据集名称
  *  ddf: 对应的数据集变量
  *  backendId: 数据集所在的后台进程　
  */
int
dexDataFrame_recordSchema(string dataframe, dexDataFrame ddf, int backendId)
{
    dexTuple* dtuples = (dexTuple*) palloc(ddf.schema->nums * sizeof(dexTuple));
    int i = 0;
    elog(INFO, "recordschema");
    ereport(INFO, (errmsg("%d", ddf.schema->nums)));
    for(; i < ddf.schema->nums; i++)
    {
        dtuples[i].nums = 4;
        dtuples[i].e = (elem*)palloc(4 * sizeof(elem));
        dtuples[i].e[0].type = 1;
        dtuples[i].e[0].value = dataframe;
        dtuples[i].e[1].type = 1;
        dtuples[i].e[1].value = ddf.schema->e[i].value;
        dtuples[i].e[2].type = 2;
        dtuples[i].e[2].value = intToString(ddf.schema->e[i].type);
        dtuples[i].e[3].type = 2;
        dtuples[i].e[3].value = intToString(backendId);
    }
    int flag =  dex_insert_table("dataFrameschema", ddf.schema->nums, dtuples);
    pfree(dtuples);
    return flag ;
}

/*
  *  dexDataFrame_recordPartitions
  *  记录dex查询过程中生成的dataframe的分区情况
  *  参数：
  *  dataframe: 数据集名称
  *  ddf: 对应的数据集变量
  *  backendId: 数据集所在的后台进程　
  */
int
dexDataFrame_recordPartitions(string dataframe, dexDataFrame ddf, int backendId)
{
    elog(INFO, "dexDataFrame recordPartitions");
    dexTuple* dtuples = (dexTuple*) palloc (sizeof(dexTuple));
    dtuples[0].e = (elem*)palloc(3 * sizeof(elem));
    dtuples[0].nums = 3;
    dtuples[0].e[0].type = 1;
    dtuples[0].e[0].value = dataframe;
    dtuples[0].e[1].type = 2;
    dtuples[0].e[1].value = intToString(ddf.partitionNum);
    dtuples[0].e[2].type = 2;
    dtuples[0].e[2].value = intToString(backendId);
    int flag = dex_insert_table("dataFramePartitions", 1, dtuples);
    pfree(dtuples);
    return flag;
}

/*
  *  dexDataFrame_getSchemaFromTable
  *  从已建立的数据表中获取数据集的schema
  *  参数：
  *  td: schema信息存储结构体
  *  dataframe: 获取schema的对象
  *  backendID: 该查询发生的后台进程
  */
int
dexDataFrame_getSchemaFromTable (dexTupledesc *td, string dfname, int backendID)
{
    char SQL[1024];
    string_init(&SQL, "SELECT colname, coltype FROM dataFrameModel WHERE dataframe = ");
    string_contat(&SQL, 4, "\'", dfname, "\' AND backendID = ", intToString(backendID));
    int nums = execStatement(&SQL);
    dexTuple *dt  = (dexTuple*) palloc(nums * sizeof(dexTuple));
    nums = dex_select_tuples(dt, &SQL);
    td->nums = nums;
    td->e = (elem*)palloc(nums*sizeof(elem));
    for(int i = 0; i < nums; i++) {
        td->e[i].value = (string)palloc(128 * sizeof(char));
        string_init(td->e[i].value, dt[i].e[0].value);
        td->e[i].type = stringToInt((string)dt[i].e[1].value);
    }
    pfree(dt);
    return 0;
}

/*
  *  dexDataFrame_getNumPartitionsFromTable
  *  从已建立的数据表中获取数据集的分区情况
  *  参数：
  *  dataframe: 获取分区数的对象
  *  backendID: 该查询发生的后台进程
  */
int
dexDataFrame_getNumPartitionsFromTable (string dataframe, int backendID)
{
    string QuerySQLQuery = (string) palloc(1024 * sizeof(char));
    string_init(QuerySQLQuery, "SELECT numPartitions FROM dataFramePartitions WHERE dataframe = ");
    string_contat(QuerySQLQuery, 4, "\'", dataframe, "\' AND backendID = ", intToString(backendID));

    dexTuple* dt = (dexTuple*)palloc(sizeof(dexTuple));
    dex_select_tuple(dt, QuerySQLQuery);
    int nums = stringToInt(dt->e[0].value);
    pfree(dt);
    return nums ;
}

/*
  *  dexDataFrame_drop
  *  当查询结束时，删除该连接中产生的数据集
  *  参数：
  *  backendID: 该查询发生的后台进程
  */
int
dexDataFrame_drop(int backendID)
{
    string DeleteSQLQuery =  "DELETE * FROM dataFrame WHERE backendID = ";
    string_contat(DeleteSQLQuery, 1,  intToString(backendID));
    return dex_drop_table_sql(DeleteSQLQuery);
}

/*
  *  dexDataFrame_join
  *  dex查询中进行数据集join操作的接口
  *  参数：
  *  dex: dex 连接信息
  *  newdataframe : join操作后产生的新的数据集
  *  dataframe1: 用于join 操作的数据集１
  *  dataframe2: 用于join操作的数据集2
  *  cols: join操作作为约束条件的列属性
  *  elist: 存储newdataframe的schema
  */
int
dexDataFrame_join(Dex *dex, string newdataframe,  string dataframe1, string dataframe2, strlist* cols, elemlist *elist)
{
    int flag = 0;
    if( ! (dexDataFrame_validate(dataframe1) > 0
            && dexDataFrame_validate(dataframe2) > 0 ))
    {
        ereport(ERROR, (errmsg("check if %s and %s is existed", dataframe1, dataframe2)));
    }
    else if(dexDataFrame_validate(newdataframe) > 0 )
    {
        ereport(ERROR, (errmsg("%s is redefined", newdataframe)));
    }
    else
    {
        dex_get(dex, MyBackendId);
        dexJoinMsg* djm = (dexJoinMsg*)palloc(sizeof(dexJoinMsg));
        djm->newDataframe = (elem*)palloc(sizeof(elem));
        djm->dataframe1 = (elem*)palloc(sizeof(elem));
        djm->dataframe2 = (elem*)palloc(sizeof(elem));
        djm->cols = (elem*)palloc(sizeof(elem));
        Dex_Join_Msg_Init(djm, newdataframe, dataframe1, dataframe2, cols);

        string buf = (string)palloc(sizeof(char)*1024);
//        ereport(INFO, (errmsg("%s", dex->host)));
        writeMessage(dex->host, dex->port, Dex_Join, djm, buf);
        int index = 0;
        string lengthStr = (string) palloc(sizeof(char) * 4);
        readString(buf, &index, lengthStr);
        int length = stringToInt(lengthStr);
        ereport(INFO, (errmsg("length: %d", length)));
        elist->nums = length;
        elist->e = (elem*)palloc(length * sizeof(elem));

        int i = 0;
        for(; i < length; i++) {
            string name = (string) palloc (sizeof(char) * 128);
            string type = (string) palloc (sizeof(char) * 2);
            readString(buf, &index, name);
            readString(buf, &index, type);
            elist->e[i].type = stringToInt(type);
            elist->e[i].value  = (string) palloc (sizeof(char) * 128);
            string_init(elist ->e[i].value, name);
        }

        //    记录新生成的dataframe
        //    dexDataFrame_recordSchema(newdataframe,  ddf);
        //    dexDataFrame_recordPartitions(newdataframe, ddf, dex.backendid);
        flag = 1;
    }
    return flag;
}

/*
  *  dexDataFrame_union
  *  dex查询中进行数据集union操作的接口
  *  参数：
  *  dex: dex 连接信息
  *  newdataframe : union操作后产生的新的数据集
  *  dataframe1: 用于union 操作的数据集１
  *  dataframe2: 用于union操作的数据集2
  */
int
dexDataFrame_union(Dex *dex, string newdataframe, string dataframe1, string dataframe2)
{
    int flag = 0;
    if( ! (dexDataFrame_validate(dataframe1) >0
            && dexDataFrame_validate(dataframe2) >0 ))
    {
        ereport(ERROR, (errmsg("check if %s and %s is existed", dataframe1, dataframe2)));
    }
    else if(dexDataFrame_validate(newdataframe) > 0)
    {
        ereport(ERROR, (errmsg("%s is redefined", newdataframe)));
    }
    else
    {
        dex_get(dex, MyBackendId);
        dexUnionMsg *dum = (dexUnionMsg*)palloc(sizeof(dexUnionMsg));
        dum->dataframe1 = (elem*)palloc(sizeof(elem));
        dum->dataframe2 = (elem*)palloc(sizeof(elem));
        dum->newDataframe = (elem*)palloc(sizeof(elem));
        Dex_Union_Msg_Init(dum, newdataframe, dataframe1, dataframe2);

        string s = (string)palloc(sizeof(char)*10);
        writeMessage(dex->host, dex->port, Dex_Union, dum,s);

//     记录新生成的dataframe
        dexDataFrame *ddf = (dexDataFrame*)palloc(sizeof(dexDataFrame));
        ddf->schema = (dexTupledesc*) palloc(sizeof(dexTupledesc));
        dexDataFrame_getSchemaFromTable(ddf->schema, dataframe1, dex->backendid);
        ddf->partitionNum = dexDataFrame_getNumPartitionsFromTable(dataframe1, dex->backendid);
        dexDataFrame_recordSchema(newdataframe,  *ddf, dex->backendid);
        dexDataFrame_recordPartitions(newdataframe, *ddf, dex->backendid);
        flag = 1;
    }

    return flag;
}


/*
  *  dexDataFrame_validate
  *  进行数据集操作前，判断该数据集是否合理
  *  参数：
  *  dataframe : 数据集
  */
int
dexDataFrame_validate(string dataframe)
{
    char SQL[1024];
    string_init(&SQL, "SELECT * FROM dataFrameModel WHERE dataframe = ");
    string_contat(&SQL, 3, "\'", dataframe, "\'");

    if (execStatement(&SQL) > 0 )
        return DATAFRAME_EXISTS;
    else
        return DATAFRAME_NOT_EXISTS;
}

/*
  *  dexDataFrame_apply
  *  dex查询中进行数据集数据分析操作的接口
  *  参数：
  *  dex: dex 连接信息
  *  dataframe: 数据集
  *  funcName: 函数名
  *  params: 数据分析函数所需参数
  */
int
dexDataFrame_apply (Dex *dex, string dataframe, string funcName, strlist* params)
{
    int flag = 0;

    if( dexDataFrame_validate(dataframe) <= 0)
    {
        ereport(ERROR, (errmsg("check if %s existed", dataframe)));
    }
    else
    {
        dex_get(dex, MyBackendId);
        dexApplyMsg *dam = (dexApplyMsg*)palloc(sizeof(dexApplyMsg));
        dam->dataframe= (elem*)palloc(sizeof(elem));
        dam->funcName = (elem*)palloc(sizeof(elem));
        dam->params = (elem*)palloc(sizeof(elem));
        Dex_Apply_Msg_Init(dam, dataframe, funcName, params);

        string s = (string)palloc(sizeof(char)*10);
        writeMessage(dex->host, dex->port, Dex_Apply, dam,s);

        flag = 1;
    }

    return flag;
}



