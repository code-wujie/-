# -
bbd中搭建数仓的一些脚本记录
主要是记录数据仓库搭建的过程

说明：
  原始数据的采集是flume从kafka采集到HDFS上，数据的格式为json格式。数据存储为普通的textfile。
  数据导入到事实表中采用定时任务
  导入到事实表中之后，利用sql将数据导入到各个维度表中



事实表表的第一张表的创建：
create external table if not exists test1(data string);
所有的维度表的格式均为一样的。创建第一张维度表：
create external table IF NOT EXISTS test2(table string,dotime string,info map<string,string>,type string) STORED AS ORC;
  info中存储的为整个json字符串转为map后的数据
  table type dotime 字段的值从json中提取出来的
  格式存储为orc格式（该格式更优）
