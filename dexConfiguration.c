/*
  *  DEXCONFIGURATION_C
  *  dex配置信息获取接口　
  */
#include "dexConfiguration.h"
#include "dexDataModel.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "executor/spi.h"
#include "executor/executor.h"

/*
  *  dexConfiguration_create
  *  创建dex配置数据表　
  */
int
create_dexconfig_table()
{
    char SQL[] = "CREATE TABLE IF NOT EXISTS DexConf(host text, port int, state int, backendid int)";
    int rs = execStatement(&SQL);
    return rs;
}

/*
  *  dexConfiguration_get
  *  dex已存在时，获取已有的dex中间件信息；
      不存在时，返回一个新建的dex中间件
  *   参数：
  *  dex: 存储产生的dex中间件的基本信息
  *  Backendid：该查询连接所在的后台进程
  */
void
dexConfiguration_get(Dex* dex, int Backendid)
{
    int rc  = 0;

    char SQL[1024] = "\0";

    string_init(&SQL,"SELECT * FROM DexConf WHERE backendid = " );
    string_contat(&SQL, 1, intToString(Backendid));

    SPI_connect();
    rc = SPI_execute(&SQL, false, 0);
    elog(INFO, "spi execute success");

    if (SPI_tuptable != NULL)
    {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable * tuptable = SPI_tuptable;

        HeapTuple tuple = tuptable -> vals[0];
        dex->host = (string) palloc(sizeof(char) * 256);
//        ereport(INFO, (errmsg("%d",strlen(dex->host))));
        string_init(dex->host,SPI_getvalue(tuple , tupdesc, 1));
//        ereport(INFO, (errmsg("%d",strlen(dex->host))));
//        ereport(INFO, (errmsg("%s",dex->host)));
        dex->port = stringToInt(SPI_getvalue(tuple, tupdesc, 2));
//        ereport(INFO, (errmsg("%d",dex->port)));
        dex->state = stringToInt(SPI_getvalue(tuple, tupdesc, 3));
        dex->backendid = stringToInt(SPI_getvalue(tuple, tupdesc, 4));
        SPI_freetuple(tuple);
    }
    SPI_finish();
}

/*
  *  dexConfiguration_set
  *  dex发生变化时，存储更新后的dex配置信息
  *   参数：
  *  dex: dex中间件的基本信息
  */
void
dexConfiguration_set(const Dex *dex)
{
    char SQL[1024] = "\0";

    string_init(&SQL,"SELECT * FROM DexConf WHERE backendid = " );
    string_contat(&SQL, 1, intToString(dex->backendid));
    int  rc = execStatement(SQL);
//    ereport(INFO, (errmsg("%d", rc)));
    if (rc == 0)
    {
        string_init(&SQL, "INSERT INTO DexConf values(\'" );              //不存在该backendid相应的dex配置，插入记录
        string_contat(&SQL, 8,
                      dex->host, 
                      "\', ", 
                      intToString(dex->port), 
                      ",", 
                      intToString(dex->state), 
                      ", " , 
                      intToString(dex->backendid), 
                      ")" );
        execStatement(&SQL);
    }
    else
    {
        string_init(&SQL, "UPDATE  DexConf SET host = \'");
        string_contat(&SQL, 7,
                      dex->host, "\', port = ", intToString(dex->port), ", state = ", intToString(dex->state),
                      "  WHERE backendid = ",  intToString(dex->backendid) );  //存在该backendid相应的dex配置，修改记录
        execStatement(&SQL);
    }
}

/*
  *  dexConfiguration_setConnState
  *  dex发生变化时，存储更新后的dex配置信息
  *   参数：
  *  dex: dex中间件的基本信息
  *  dexConnState: 更新dex中间件的连接状态
  */
void
dexConfiguration_setConnState(Dex *dex, int dexConnState)        //修改dex的连接状态
{
    char SQL[1024] = "\0";
    string_init(&SQL, "UPDATE DexConf SET state = ");
    string_contat(&SQL,  3,
                  intToString(dexConnState), "  WHERE backendid = " , intToString(dex->backendid));
    execStatement(&SQL);
}
