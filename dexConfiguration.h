/*
  * DEXCONFIGURATION_H
  *  DEX基本信息的存储以及在DEX基本信息的更新
  */

#ifndef  _DEXCONFIGURATION_H
#define _DEXCONFIGURATION_H

#include "dexBase.h"

//建立相关表格存储dex基本信息
int
dexConfiguration_create();

//dex信息重用时，获取dex信息
void
dexConfiguration_get(Dex *dex, int Backendid);

//当查询过程中dex信息发生变化时，重置dex
void
dexConfiguration_set(const Dex *dex);

//当dex查询连接的状态发现改变时，更改连接状态
void
dexConfiguration_setConnState(Dex *dex, int dexConnState);

#endif // _DEXCONFIGURATION_H
