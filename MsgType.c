/*
 * MSG_C
 * 消息基本类型的操作
 */
#include "MsgType.h"
#include "postgres.h"

/*
 * getTypeName
 * 依据消息类型值获取消息类型
 * DType: Dex协议中规定的消息类型值
 */
string
getTypeName(DType dt){
    string s;
    switch (dt) {
        case Dint: s = "int"; break;
        case Dstring: s = "string"; break;
        case Dfloat: s = "float"; break;
        case Dintlist: s = "int[]"; break;
        case Dstringlist: s = "string[]"; break;
        case Delemlist: s = "elem[]"; break;
    }
    return s;
}

/*
 * getType
 * 依据消息类型名获取消息类型值
 * s: Dex协议中规定的消息类型名
 */
DType
getType(string s) {
    if(strcmp(s, "text") == 0)  {
        return Dstring;
    } else if (strcmp(s,  "int4") == 0) {
        return Dint;
    } else if (strcmp(s, "float")) {
        return Dfloat;
    }
}

/*
 * getSize
 * 根据消息类型判断消息结构的元素
 * mt: 消息类型
 */
int
getSize(MsgType mt){
    int num = 0;
    switch(mt){
        case Dex_Connect: num = 1; break;
        case Dex_Disconnect:num= 1; break;
        case Dex_Session:num = 1; break;
        case Dex_DataFrame:num =  4; break;
        case Dex_DataFrame_Partition:num = 2; break;
        case Dex_DataFrame_Repartition :num= 4; break;
        case Dex_DataFrame_Iterator : num= 3; break;
        case Dex_Join : num= 4; break;
        case Dex_Union:num  = 3; break;
        case Dex_Apply:num = 3; break;
    }
    return num ;
}

