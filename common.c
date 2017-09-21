/*
  * COMMAN_C
  *  常用数据类型的操作
  */
#include <stdarg.h>
#include "common.h"
#include "postgres.h"
#include<string.h>

/*
  * string_init
  *  字符串初始化
  *  参数：
  *  s1:  待初始化字符串
  *  s2:  用于初始化的字符串
  */
void
string_init(string s1, string s2) {
    string_copy(s1, s2, strlen(s2));
}

/*
  * string_contat
  *  字符串连接
  *  参数：
  *  s：连接后的字符串结果
  *  num: 连接字符串个数
  *  s1:  添加到字符串s的字符串
  */
void
string_contat(string s, int num, string s1, ...) {
    va_list ap;
    va_start(ap, s1);
    strcat(s, s1);
    int i = 1;
    for (; i<num ; i++) {
         string tmp = va_arg(ap, string);
        strcat(s, tmp);
//        strcat(s, );
    }
    va_end(ap);
}

/*
  * string_copy
  *  字符串复制
  *  参数：
  *  s1: 复制后新的字符串
  *  s2: 待被复制的字符串
  *  length：复制长度
  */
void
string_copy(string s1, string s2, int length) {
    memcpy(s1, s2, length);
    s1[length]= '\0';
}

char *
string_dup(char *from) {
    int len = strlen(from);
    char *s = palloc(len + 1);
    string_copy(s, from, len);
    s[len] = '\0';
    return s;
}

/*
  * string_split
  *  字符串分割
  *  参数：
  *  s：待分割的字符串
  *  c: 分割标识符
  * 返回值：字符串数组
  */
// string*
// string_split(string s, char c) {
//     int i = 0;
//     string* arr = (string*) malloc(2*sizeof(string));
//     int count = 0;
//     int start = 0;
//     for (; i < strlen(s); i++) {
//         if(s[i] == c) {
//             arr[0] = (string) malloc(64);
//             string_copy( arr[0], &s[start], i - start);
//             if (strlen(s) > i+1){
//                 arr[1] = (string) malloc(64);
//                 string_copy(arr[1], &s[i+1], strlen(s) - i -1);
//             } else {
//                 arr[1] = NULL;
//             }
//         }
//     }
//     return arr;
// }

char **
string_split(char *s, char delim) {
    char c;
    char  *buffer;
    char **strlist;
    int  n = 1;
    int  size = 1;

    buffer = palloc(strlen(s)+1);

    for (int i = 0; s[i] != '\0'; i++) {
        if (s[i] == delim) {
            buffer[i] = '\0';
            n++;
        } else {
            buffer[i] = s[i];
        }
    }
    
    strlist = (char **) palloc((n * sizeof(char *)));
    
    n = 0;
    strlist[n] = s;
    while ( (c = *s++) != '\0') {
        if (c == delim) {
            strlist[n++] = s;
        }
    }

    return strlist;
}

/*
  * string_isequal
  *  判断字符串是否相等
  *  参数：
  *  s1: 字符串１
  *  s2: 字符串２
  *  返回值：0 ： 不相等；１：相等
  */
int
string_isequal(string s1, string s2) {
    int flag= 0;
    if (strcmp (s1, s2) == 0) {
        flag = 1;
    }
    return flag;
}

/*
  * stringToInt
  *  将字符串转化为数字
  *  参数：
  *  s : 待转化字符串
  *  返回值：整型数值
  */
int
stringToInt(const string s) {
    return atoi(s);
}

/*
  *  intToString
  *  将数字转化为字符串
  *  参数：
  *  a : 待转化的整数
  *  返回值：转化后的字符串
  */
string
intToString(int a) {
    string s = (string) malloc(256);
    snprintf(s, 256, "%d", a);
    return s;
}

strlist *
strlist_new(int capacity) {
    strlist *sl = (strlist*) palloc(sizeof(strlist));
    sl->count = 0;
    sl->num = 0;
    sl->list = NULL;
    if (capacity > 0) { 
        sl->list = (string*) palloc(capacity * sizeof(string));
        sl->num = capacity;
    }
    return sl;
}

void
strlist_free(strlist *sl) {
    if (sl->num)
        pfree(sl->list);
    pfree(sl);
}

void
strlist_add(strlist* sl, string s) {
    int length = 0;
    while (sl->count >= sl->num) {
        strlist_enlarge(sl);
    }
    sl->list[sl->count++] = s;
}

int
list_enlarge(strlist *sl) {
    int n = MAX(sl->num * 2, 1);
    sl->list = (string*) repalloc(sl->list, n);
    sl->num = n;
    return n;
}

const char**
list_copy_crude(char **s, int n) {
    char **new, **p;
    p = new = (char**) palloc(n*sizeof(char *));
    while (n-- > 0)
        *p++ = *s++;

    return new;
}

const cstrlist *
build_cstrlist(strlist *sl) {
    int n = sl->count;
    cstrlist *csl = (cstrlist *)palloc(sizeof(cstrlist));
    csl->num = n;
    csl->list = list_copy_crude(sl->list, sl->count);
    return csl;
}



