/*
 *  DEXDATAMODEL_C
 *  dex 与数据库中数据表的数据处理api
 */
#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/geo_decls.h"

#include "dexDataModel.h"

/*
  *  execStatement
  *  执行sql语句
  *  参数：
  *  sql: 待执行的sql语句　
  */
void*
execStatement(string sql)
{
    int rs = 0;
    int proc = -1;
    if ((rs = SPI_connect())<0)
    {
        elog(ERROR, "SPI connect failure!");
        exit(1);
    }
    rs = SPI_execute(sql, false, 0);
//    elog(INFO, "success");
    proc = SPI_processed;
    if (rs < 0 )
    {
        elog(ERROR, "Unvalid sql statement!");
        SPI_finish();
        exit(1);
    }
    SPI_finish();
    return proc;
}

/*
  *  dex_get_model
  *  获取指定dataframe的schema
  *  参数：
  *  dtd : 存储返回的schema信息
  *  sql : 查询语句　　
  */
int
dex_get_model(dexTupledesc* dtd, string sql )
{
    int rs = 0;
    if ( (rs = SPI_connect()) < 0)
    {
        elog(ERROR, "SPI connect failure!");
        exit(1);
    }

    rs = SPI_execute(sql, TRUE, 0);
    if(rs < 0)
    {
        elog(ERROR, "Unvalid table or SQL statement");
        SPI_finish();
        exit(1);
    }
//    elog(INFO, "execute sucessfully");

    if (SPI_tuptable!=NULL)
    {
//        elog(INFO, " tuptable is not NULL");
        TupleDesc td = SPI_tuptable->tupdesc;
        SPITupleTable *tt = SPI_tuptable;

        int nums = td->natts;
//        ereport(INFO, (errmsg("%d",nums)));
        dtd->nums = nums;
        dtd->e = (elem*)palloc(nums * sizeof(elem));

        int i = 1;
        for (; i <= td->natts; i ++)
        {
            dtd->e[i-1].type =  getType(SPI_gettype(td, i));
            dtd->e[i-1].value = (string)palloc(sizeof(char)*1024);
            string_init(dtd->e[i-1].value, SPI_fname(td, i));
        }
    }

    SPI_finish();
    return 1;
}

/*
  *  未实现
  *  dex_get_data
  *  获取dataframe中指定分区的数据内容
  *  参数：
  *  dtd : 存储返回的schema信息
  *  sql : 查询语句　　
  */
int
dex_get_data(int partitionID, string data)
{
    return 0;
}

/*
  *  未实现
  *  dex_partition_data
  *  对数据集的数据内容进行分割
  *  参数：
  *  numsPartition: 分区数　　
  */
int
dex_partition_data(int numsPartition)
{
    return 0;
}

/*
  *  insert_element
  *  插入数据元素
  *  参数：
  *  sql: 插入数据的sql查询语句　　
  *  e: 待插入的元素　
  */
int
insert_element(string sql, elem e)
{
    switch (e.type)
    {
    case 1:
        string_contat(sql, 3, "\'", e.value, "\'");
        break;
    case 2:
        string_contat(sql, 1, e.value);
        break;
    default:
        elog(ERROR, "ERROR TUPLE");
        exit(1);
    }
    return 0;
}

/*
  *  dex_insert_tuple
  *  插入数据元组
  *  参数：
  *  s: 插入数据的sql查询语句　　
  *  tuple:  待插入的元组　
  */
int
dex_insert_tuple(string s, dexTuple dtuple)
{
    elog(INFO, "dex_insert_tuple");
    string_contat(s, 1, "(");
    int i = 0;
    for(; i< dtuple.nums -1; i++)
    {
        insert_element(s, dtuple.e[i]);
        string_contat(s, 1, ",");
    }
    insert_element(s, dtuple.e[i]);

    string_contat(s,1, ")");
    return 1;
}

/*
  *  dex_insert_table
  *  在表格中插入多组数据元组
  *  参数：
  *  table:  插入数据的数据表 　
  *  num:  待插入的元组个数
  *  dtuples: 待插入的元组　
  */
int
dex_insert_table(string table, int num,  dexTuple* dtuples)
{
    string sql = (string) palloc(1024);
    string_init(sql, "INSERT INTO ");
    string_contat(sql, 2, table, " VALUES ");
    int i = 0;

    for(; i<num -1; i++)
    {
        dex_insert_tuple(sql, dtuples[i]);
        string_contat(sql, 1, ",");
    }
    dex_insert_tuple(sql, dtuples[i]);
    int rs = execStatement(sql);
    pfree(sql);
    return rs;
}

/*
  *  create_element
  *  根据元素创建数据表的列
  *  参数：
  *  sql: 新建数据表的sql 语句
  *  e: 数据表列属性元素　　
  */
int
create_element(string sql , elem e)
{
    switch(e.type)
    {
    case 1:
        string_contat(sql, 2, e.value, " text");
        break;
    case 2:
        string_contat(sql, 2, e.value, " int");
        break;
    default:
        elog(ERROR, "error elements");
        exit(1);
    }
    return 0;
}

/*
  *  dex_create_table
  *  新建表格
  *  参数：
  *  table: 数据表名
  *  dtdesc: 数据表列属性　　
  */
int
dex_create_table(string table, dexTupledesc dtdesc)
{
    string sql = "CREATE IF NOT EXISTS TABLE ";
    string_contat(sql , 2, table, "(");
    int i = 0 ;
    for (; i < dtdesc.nums - 1; i ++)
    {
        create_element(sql, dtdesc.e[i]);
        string_contat(sql, 1, ", ");
    }
    create_element(sql, dtdesc.e[i]);
    string_contat(sql, 1, ")");

    int rs = execStatement(sql);
    free(sql);
    return rs;
}

/*
  *  dex_drop_table
  *  删除数据表
  *  参数：
  *  table: 数据表名　
  */
int
dex_drop_table(string table)
{
    string sql = (string)malloc(1024);
    string_init (sql, "DROP TABLE IF EXISTS ");
    string_contat(sql, 1, table);
    int rs = execStatement(sql);
    free(sql);
    return rs;
}


/*
  *  dex_select_table
  *  获取数据表内容
  *  参数：
  *  table: 获取数据表对象
  * sl : 指定列属性
  */
int
dex_select_table(string table, strlist*sl)
{
    int num = sl ->num;
    int i = 0 ;
    string sql = "SELECT ";
    if (num == 0)
    {
        string_contat(sql, 2, "* FROM ", table);
    }
    else
    {
        for( ; i< num - 1; i++)
        {
            string_contat(sql, 2,  sl->list[i], ",");
        }
        string_contat(sql, 2, sl->list[i], "  FROM");
        string_contat(sql, 1, table);
    }
    int proc = execStatement(sql);
    return proc;
}

/*
  *  dex_select_tuple
  *  获取数据表一个元组
  *  参数：
  *  dt:　存储查询到的元组
  *  sql:　查询语句　
  */
int
dex_select_tuple(dexTuple* dt, string sql)
{
    int rc = 0;
    if ((rc = SPI_connect()) < 0)
    {
        elog(ERROR, "Connection failure!");
        exit(1);
    }
    SPI_exec(sql, 0);
    int proc = SPI_processed;
    if(SPI_tuptable != NULL)
    {
        elog(INFO, "SPI_tuptable");
        TupleDesc td = SPI_tuptable ->tupdesc;
        SPITupleTable *tt = SPI_tuptable;
        HeapTuple tuple = tt->vals[0];
        int i = 1 ;
        dt->nums = td->natts;
        dt->e = (elem*)palloc(td->natts*sizeof(elem));
        for(; i <= td->natts; i++)
        {
            dt->e[i-1].type = getType(SPI_gettype(td, i));
            dt->e[i-1].value = SPI_getvalue(tuple, td, i);
        }
    }
    SPI_finish();
    return proc;
}


/*
  *  dex_select_tuples
  *  获取数据表多个元组
  *  参数：
  *  dt:　存储查询到的元组
  *  sql:　查询语句　
  */
int
dex_select_tuples(dexTuple *dt, string sql)
{
    int rc = 0;
    if ((rc = SPI_connect()) < 0)
    {
        elog(ERROR, "Connection failure!");
        exit(1);
    }
    SPI_exec(sql, 0);
    int proc = SPI_processed;
    if(SPI_tuptable != NULL)
    {
        TupleDesc td = SPI_tuptable ->tupdesc;
        SPITupleTable *tt = SPI_tuptable;
        int j =0;
        for(; j < proc; j++)
        {
            HeapTuple tuple = tt->vals[j];

            dt[j].nums = td->natts;
            dt[j].e = (elem*)palloc((td->natts)*sizeof(elem));
            int i = 1 ;
            for(; i <= td->natts; i++)
            {
                dt[j].e[i-1].type = getType(SPI_gettype(td, i));
                dt[j].e[i-1].value = (string)palloc(128 * sizeof(char));
                string_init(dt[j]. e[i-1].value, SPI_getvalue(tuple, td, i));
            }
        }
    }
    SPI_finish();
    return proc;
}
