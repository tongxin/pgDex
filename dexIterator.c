/*
 * DEXITERATOR_C（需重新设计）
 * 获取并记录dex查询过程中数据集的元素指针
 */
#include "dexIterator.h"
#include "dexBase.h"
#include "dexDataModel.h"

/*
 * dexIterator_createTable
 * 建立存储iterator与dataframe对应关系的数据表
 */
int
dexIterator_createTable(){
    return dex_create_table_sql("CREATE TABLE IF NOT EXISTS dexIterator (iterator text, dataframe text, backendid int)");
}

/*
 * dexIterator_record
 * 记录生成的
 * 变量：
 * iter: 迭代指针
 * dataframe: iter对应的数据集
 * backendId: 该操作所在后台进程的编号，方便查询
 */
int
dexIterator_record(string iter, string dataframe, int backendId){
    elemlist elist; 
    elist->nums = 3;
    elist->e = (elemlist)malloc(elist->nums);
    elist->e[0].type = 1;
    elist->e[0].value = iter;
    elist->e[1].type = 1;
    elist->e[1].value = dataframe;
    elist->e[2].type = 2;
    elist->e[2].value = backendId;
    return dex_insert_table("dexIterator",elist->nums, elist->e);
}


/*
 * _dexIterator_create
 * iterator创建实现
 * 变量：
 * iter: 迭代指针
 * dataframe: iter对应的数据集
 * dex: dex连接信息,可用于向spark发送生成iterator请求
 */
string
_dexIterator_create(string iterator, string dataframe, Dex dex){
    dexDataFrameIterMsg ddfim = Dex_DataFrame_Iteration_Msg_Init(dataframe, partitionID, iteratorID);
    string s = (string)writeMessage(dex.host, dex.port, Dex_DataFrame_Iterator, ddfim);
    dexIterator_record(iterator,dataframe, dex.backendid);
    //dexIterator_create()    //向大数据分析平台发送请求建立指定dataframe的iterator(待实现)
    return s;
}

/*
 * _dexIterator_create
 * iterator创建
 * 变量：
 * iter: 迭代指针
 * dataframe: iter对应的数据集
 * dex: dex连接信息,可用于向spark发送生成iterator请求
 */
string
dexIterator_create(string iterator, string dataframe,Dex dex) {
    return _dexIterator_create(iterator, dataframe, dex);
}

/*
 * dexIterator_validate
 * 检验iterator是否存在
 * 变量：
 * iterator: 迭代指针
 */
bool
dexIterator_validate(string iterator) {
    string command = "SELECT * FROM dexIterator WHERE iterator = ";
    string_contat(command, iterator);
    int num = dex_select_table_sql(command);
    if (num >0 )
        return 1;
    else
        return 0;
}
