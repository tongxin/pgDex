/*
  *  CLUSTERHOST_C
  *  数据库后台对集群节点IP的管理
  */
#include "clusterHost.h"
#include "dexDataModel.h"
#include <stdio.h>
#include <unistd.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/geo_decls.h"

/*
  *  clusterHost_readConfig
  *  从大数据分析平台集群节点配置文件读取集群节点信息
  *  参数：
  *  hosts: 集群节点连接信息
  *  fileName: 配置文件
  */
void
clusterHost_readConfig(strlist* hosts, string fileName) {
        elog(INFO, "clusterHost_readConfig");
        FILE *fp;
        ereport(INFO, (errmsg("%s",fileName)));
        if ((fp = fopen(fileName, "r")) == NULL) {
            elog(INFO,"fail to read");
            exit(1);
        }
        while(true) {
            string tmp = (string) malloc(64);
            if (fgets(tmp, 64, fp) != NULL) {
                tmp[strlen(tmp)-1] = '\0';
//                ereport(INFO,(errmsg("%s", tmp)));
                 strlist_add(hosts,  tmp);
            } else
                break;
        }
        fclose(fp);
        fp = NULL;
//        ereport(INFO,(errmsg("%s",hosts->list[0])));
}

/*
  *  create_clusterhost_table
  *  建立表格存储从文件中获取的集群节点信息
  */
int
create_clusterhost_table() {
    char SQL[] = "CREATE TABLE IF NOT EXISTS clusterHost(host text, port int, state int)";
    int rs = execStatement(&SQL);
    return rs;
}

/*
  *  drop_clusterhost_table
  *  配置文件更新时，删除原有的配置信息，重新建立
  */
int
drop_clusterhost_table() {
    char SQL[] = "DROP TABLE IF EXISTS clusterHost";
    return execStatement(&SQL);
}

/*
  *  clusterHost_insert
  *  在表格中插入从配置文件中获取的数据
  *  参数：
  *  hosts: 集群节点连接信息
  */
int
clusterHost_insert(strlist* hosts) {
    int nums = hosts->count;
    dexTuple* dtuples = (dexTuple*) palloc(nums * sizeof(dexTuple));

    int i = 0;
    for(; i< nums; i++) {
        string* arr = string_split(hosts->list[i], ':');
        dtuples[i].nums = 3;
        dtuples[i].e = (elem*)palloc(3*sizeof(elem));
        dtuples[i].e[0].type = 1;
        dtuples[i].e[0].value = arr[0];
        dtuples[i].e[1].type = 2;
        dtuples[i].e[1].value = arr[1];
        dtuples[i].e[2].type = 2;
        dtuples[i].e[2].value = "0";
    }
    int rc = dex_insert_table("clusterHost", nums, dtuples);
    i = 0;
     for(; i< nums; i++) {
      pfree(dtuples[i].e);
      };
    pfree(dtuples);
    return rc;
}

/*
  *  clusterHost_update
  *  当集群被使用，与数据库建立连接时，连接状态改变
  *  参数：
  *  ch: 集群master节点
  *  state: 连接状态
  */
int
clusterHost_update(clusterHost ch, int state) {
    char SQL[1024] = "UPDATE clusterHost SET state = ";
    string_contat(&SQL,  5, intToString(state), "  WHERE host = \"", ch.hostname, "\" AND port = ", intToString(ch.port));
    int proc = dex_update_table_sql(&SQL);
    return proc;
}

/*
  *  load_clusterhost_config
  *  更新大数据分析平台的配置文件　
  */
void
load_cluster_config(){
    elog(INFO, "load_cluster_config");
    drop_clusterhost_table();
    create_clusterhost_table();
    char filename[1024];
    getcwd(&filename, 1024);
    string_contat(&filename, 1,"/dex_cluster.conf");
    strlist *hosts = strlist_new(2);
    clusterHost_readConfig(hosts, &filename);
    clusterHost_insert(hosts);
    strlist_free(hosts);
}

/*
  *  clusterHost_get
  *  获取合适的集群节点信息　
  *  参数：
  *  ch：用于存储集群节点信息
  */
clusterHost
clusterHost_get(){
    clusterHost ch;
    char SQL[] = "SELECT host, port FROM clusterHost where state = 0 limit 1";
    dexTuple *dtuples = (dexTuple*)palloc(sizeof(dexTuple));
    int nums = dex_select_tuple(dtuples, &SQL);
    string_init(&ch.hostname[0], (string)dtuples->e[0].value);
    ch.port = stringToInt(dtuples->e[1].value);
//    elog(INFO, "get clusterHost");
    return ch;
}

/*
  *  clusterHost_validate
  *  验证该节点host是否有效　
  *  ch: 指定集群节点
  */
int
clusterHost_validate(clusterHost *ch) {
    string QuerySQLQuery = "SELECT * FROM clusterHost where host = \"";
    string_contat(QuerySQLQuery, 4, ch->hostname, "\" AND port = ", ch->port, " AND state = 1");
    int proc = dex_select_table_sql(QuerySQLQuery);
    if(proc >0) {
        return 1;
    } else {
        return -1;
    }
}

/*
  *  clusterHost_free
  *  释放已使用的大数据分析平台的集群节点HOST　
  *  ch: 已使用的集群节点host
  */
int
clusterHost_free(clusterHost ch) {
    clusterHost_update(ch, 0);
    return 0;
}



