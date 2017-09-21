/*
 * FUN_C
 * 数据库的UDF函数
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "storage/backendid.h"
#include "funcapi.h"

#include "dex.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(Dex_start);

Datum
Dex_start(PG_FUNCTION_ARGS)
{
    elog(INFO,"Dex_start");

    dex_start();

    PG_RETURN_INT32(0);
}


PG_FUNCTION_INFO_V1(Dex_connect);
//调用dex后台进行连接　
Datum
Dex_connect(PG_FUNCTION_ARGS)
{
    elog(INFO, "dex_connect");
 
    // bool flag = true;
    dex_connect();

    PG_RETURN_INT32(1);
}

PG_FUNCTION_INFO_V1(Dex_disconnect);
//进程结束Dex操作，断开连接
Datum
Dex_disconnect(PG_FUNCTION_ARGS)
{
    elog(INFO, "Dex_disconnect");

    Dex dex;;
    dex_get(&dex, MyBackendId);
    if(dex.state == DEXCONNECTED) {
        dex_disconnect(&dex);
        PG_RETURN_INT32(1);
    }
}

//PG_FUNCTION_INFO_V1(Dex_createSession);
////生成session支持后续操作
//Datum
//Dex_createSession(PG_FUNCTION_ARGS) {
//    Dex* dex = (Dex*)palloc(sizeof(Dex));
//    dex_get(dex, MyBackendId);
//    bool flag = false;
//    if (PG_NARGS() > 0) {
//        flag = dex_createSessionWithPath(dex, text_to_cstring(PG_GETARG_TEXT_P(0)));    //cluster 的启动路径
//    } else {
//        flag  = dex_createSession(dex);
//    }
//        PG_RETURN_INT32(1);
//}

PG_FUNCTION_INFO_V1(Dex_createDataFrame);
//生成数据集
Datum
Dex_createDataFrame(PG_FUNCTION_ARGS)
{
    Dex* dex = (Dex*) palloc(sizeof(Dex));
    dex_get(dex, MyBackendId);
    bool flag = false;
    string dataframeName = text_to_cstring(PG_GETARG_TEXT_P(0));
    if(PG_NARGS()>3)
    {
        if (_dex_createDataFrame(*dex,  dataframeName,                                     //dataframe的标识 符
                                        text_to_cstring(PG_GETARG_TEXT_P(1)),       //url 数据库所在host 及 database name
                                        text_to_cstring(PG_GETARG_TEXT_P(2)),      //table name
                                        PG_GETARG_INT32(3)                    //partitions
                                       ) > 0)
            flag = true;
    }
    else
    {
        elog(INFO, "dex_createDataFrame");
        if ( dex_createDataFrame(*dex, dataframeName,                                          //dataframe的标识符
                                 text_to_cstring(PG_GETARG_TEXT_P(1)),           //url 数据库所在host 及 database name
                                 text_to_cstring(PG_GETARG_TEXT_P(2))            //table name
                                ) >0 )
            flag = true;
    }
    PG_RETURN_BOOL(flag);
}


PG_FUNCTION_INFO_V1(Dex_joinDataFrame);
//DataFrame 的join 操作
Datum
Dex_joinDataFrame (PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    int                  call_cntr;
    int                  max_calls;
    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;

    Dex* dex = (Dex*)palloc(sizeof(Dex));
    elemlist *elist = (elemlist*) palloc (sizeof(elemlist));
    int count ;

    if (SRF_IS_FIRSTCALL())
    {
        bool flag = false;
        string newdf =  text_to_cstring(PG_GETARG_TEXT_P(0));
        string df1 =  text_to_cstring(PG_GETARG_TEXT_P(1));
        string df2 =  text_to_cstring(PG_GETARG_TEXT_P(2));

        ArrayType* arr = PG_GETARG_ARRAYTYPE_P(3);
        int ndims = ARR_NDIM(arr);
        int *dims = ARR_DIMS(arr);
        int nitems = ArrayGetNItems(ndims, dims);
        ereport(INFO, (errmsg("%d,%d,%d",ndims,*dims, nitems)));

        strlist *arrlist = strlist_new(nitems);
        for(int i = 0; i <= nitems; i++)
        {
            Datum d;
            string val;
            bool isnull;
            d = array_ref(arr, 1, &i, -1, -1, false, 'i', &isnull);
            val = TextDatumGetCString(d);
            strlist_add(arrlist, val);
        }
        elog(INFO, "flag");
        flag =  dex_joinDataFrame(dex,
                                  newdf,
                                  df1,            //dataframe1  的标识符
                                  df2,             //dataframe2 的标识符
                                  arrlist ,                                //列名，join 需满足， dataframe1.col = dataframe2.col
                                  elist
                                 ) >0? true : false;

        MemoryContext   oldcontext;

        /* 创建一个函数环境，用于在调用间保持住 */
        funcctx = SRF_FIRSTCALL_INIT();

        /* 切换到适合多次函数调用的内存环境 */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        count = 0;
        /* 要返回的行总数 */
        funcctx->max_calls = elist->nums;

        /* 为的结果类型制作一个行描述 */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        /* 生成稍后从裸 C 字符串生成行的属性元数据 */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        MemoryContextSwitchTo(oldcontext);
    }

    /* 每次函数调用都要做的事情 */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)    /* 在还有需要发送的东西时继续处理 */
    {
        count = call_cntr;
        char       **values;
        HeapTuple    tuple;
        Datum        result;

        /*
         * 准备一个数值数组用于版本的返回行。
         * 它应该是一个C字符串数组，稍后可以被合适的类型输入函数处理。
         */
        values = (char **) palloc(2 * sizeof(char *));
        values[0] = (char *) palloc(128 * sizeof(char));
        values[1] = (char *) palloc(128 * sizeof(char));

        string_init(values[0] , elist->e[count].value);
        string_init(values[1], getTypeName(elist->e[count].type));

        /* 制作一个行 */
        tuple = BuildTupleFromCStrings(attinmeta, values);

        /* 把行做成 datum */
        result = HeapTupleGetDatum(tuple);

        /* 清理(这些实际上并非必要) */
        pfree(values[0]);
        pfree(values[1]);
        pfree(values);
        SRF_RETURN_NEXT(funcctx, result);
    }
    else    /* 在没有数据残留的时候干的事情 */
    {
        SRF_RETURN_DONE(funcctx);
    }

}

PG_FUNCTION_INFO_V1(Dex_unionDataFrame);
//DataFrame 的union 操作
Datum
Dex_unionDataFrame(PG_FUNCTION_ARGS)
{
    bool flag = false;
    Dex *dex = (Dex*)palloc(sizeof(Dex));
    flag = dex_unionDataFrame(dex,
                              text_to_cstring(PG_GETARG_TEXT_P(0)),       //newdataframe的标识符
                              text_to_cstring(PG_GETARG_TEXT_P(1)),       //dataframe1 的标识符
                              text_to_cstring(PG_GETARG_TEXT_P(2))        //dataframe2 的标识符
                             ) >0 ? true : false;
    PG_RETURN_BOOL(flag);
}

PG_FUNCTION_INFO_V1(Dex_applyFunctions);

Datum
Dex_applyFunctions(PG_FUNCTION_ARGS)
{
    bool flag = false;
    Dex *dex = (Dex*)palloc(sizeof(Dex));
    string df =  text_to_cstring(PG_GETARG_TEXT_P(0));
    string funcName =  text_to_cstring(PG_GETARG_TEXT_P(1));

    ArrayType* arr = PG_GETARG_ARRAYTYPE_P(2);
    int ndims = ARR_NDIM(arr);
    int *dims = ARR_DIMS(arr);
    int nitems = ArrayGetNItems(ndims, dims);

    strlist *arrlist = strlist_new(nitems);
    for(int i = 1; i <= nitems; i++)
    {
        Datum d;
        string val;
        bool isnull;
        d = array_ref(arr, 1, &i, -1, -1, false, 'i', &isnull);
        val = TextDatumGetCString(d);
        strlist_add(arrlist, val);
    }
    elog(INFO, "flag");

    flag = dex_applyFunctions(dex,
                              df ,       //dataframe的标识符
                              funcName ,    //function 的名称
                              arrlist
                             ) >0 ? true : false;
    PG_RETURN_BOOL(flag);
}


