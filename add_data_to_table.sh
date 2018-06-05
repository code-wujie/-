#!/bin/bash

echo "start--------------------------------------------------------start"

#检查参数
if [ $# -ne 1 ];then
    echo "参数不正确！${table}"
    exit 1
fi
#当前系统时间
current_time=`date '+%Y%m%d_%H%M%S'`

#基础路径
base_path=$(pwd)
echo "当前路径:${base_path}"
#参数表名（待处理数据表）
table=$1
echo "当前处理的表名:${table}"

#爬虫数据接入库/原始数据仓库
database_01=db_crawler
database_02=db_crawler_ods
echo "爬虫数据接入库数据库名:${database_01}"
echo "爬虫原始数据仓库库名:${database_02}"

#hive jdbc url
hive_url="jdbc:hive2://dataompro06.test.bbdops.com:10000"
echo "hive_jdbc_url=${hive_url}"

#相关用户（bbdoffline 查询等离线操作用户，dataomwarehouse DDL用户）
user_name1=bbdoffline
user_name2=dataomwarehouse
echo "用户名1:${user_name1}"
echo "用户名2:${user_name2}"

#密码
passwd=""
echo "密码:******${passwd}"

#bbd_table值域查询sql语句
sql_bbd_table="select distinct(get_json_object(data,'$.bbd_table')) as bbd_table from ${database_01}.${table}"
echo "sql语句:${sql_bbd_table}"

#存放查询出来的bbd_table列表
bbd_table_list_path=${base_path}/exec_logs/${table}_${current_time}_bbd_table_list
echo "bbd_table列表存放文件:${bbd_table_list_path}"

#存放查询出来的bbd_table列表
bbd_table_list_path=${base_path}/exec_logs/${table}_${current_time}_bbd_table_list
echo "bbd_table列表存放文件:${bbd_table_list_path}"

#存放建表语句列表
create_table_hql=${base_path}/exec_logs/${table}_${current_time}_hql_table_create.hql
echo "建表语句脚本:${create_table_hql}"

#存放转换语句
tran_table_hql=${base_path}/exec_logs/${table}_${current_time}_hql_table_tran.hql
echo "转换语句脚本:${tran_table_hql}"
echo "初始化转换语句！"
echo "from (select bbd_data_unique_id,info,bbd_table,bbd_acctime,bbd_type from (select t1.bbd_data_unique_id,t1.info,t1.bbd_table,t1.bbd_acctime,t1.bbd_type,row_number() over(partition by bbd_data_unique_id order by bbd_acctime desc) rn from ${database_01}.${table}) t1 where t1.rn=1) t2">>${tran_table_hql}


#执行hql
echo "开始执行bbd_table值域查询sql语句:${sql_bbd_table}"
beeline -u "${hive_url}/${database_01}" -n ${user_name1} -p "${passwd}" -e "${sql_bbd_table}"|sed 1,3d|tac|sed 1d|tac|sed 's/[ |]//g'>${bbd_table_list_path}

if [ $? -ne 0 ]; then
    echo "bbd_table列表查询失败！"
    exit 1
fi

#构建hql语句
while read bbd_table
do
    echo "create external table if not exists ${database_02}.${bbd_table} like ${database_02}.baidu_news;" >>${create_table_hql}
    echo "insert into table ${database_02}.${bbd_table} partition(bbd_type) select bbd_data_unique_id,str2map(info),bbd_table,bbd_acctime,bbd_type where bbd_table=\"${bbd_table}\"">>${tran_table_hql}

done<${bbd_table_list_path}

echo "开始执行建表语句:${bbd_table}"
beeline -u "${hive_url}/${database_02}" -n ${user_name2} -p "${passwd}" -f "${create_table_hql}"

if [ $? -ne 0 ]; then
    echo "建表失败！table_name:${database_02}.${bbd_table}"
else
    echo "建表成功！"
fi

echo ""
echo "开始执行数据转换语句！"
hive -i init.hql -f ${tran_table_hql}


if [ $? -ne 0 ]; then
    echo "数据处理失败！${tran_table_hql}"
else
    echo "数据处理成功！${tran_table_hql}"
fi

echo "end--------------------------------------------------------end" 


