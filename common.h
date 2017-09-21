/*
  *  COMMAN_H
  *  常用数据类型的操作
  *  字符串string 相关操作，方便进行字符串消息处理
  *  字符串数组strlist相关操作，方便进行多个字符串参数的消息类型处理
  *   整数数组intlist相关操作，方便进行多个整数参数的消息类型处理
  */

#ifndef _COMMON_H
#define _COMMON_H


typedef char* string;

void
string_init(string s1, string s2) ;

void
string_contat(string s, int num,  string s1, ...);

void
string_copy(string s1, string s2, int length);

string*
string_split(string s, char  c);



int
string_isequal(string s1, string s2);

int
stringToInt(string s);

string
intToString(int a);

typedef char** array;

typedef unsigned char byte;

struct strlist {
    int num;
    int count;
    string* list;
};
typedef struct strlist strlist;

struct cstrlist {
    int num;
    const string* list;
};

typedef struct cstrlist cstrlist;

strlist *
strlist_new(int c);

void
strlist_init(strlist* s) ;

void
strlist_add(strlist* sl, string s);

const cstrlist *
build_cstrlist(strlist *s);

typedef int* intlist;

#define MAX(a, b) ((a) > (b) ? (a) : (b))

#endif // _COMMON_H

