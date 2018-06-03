echo '==========start to create temp table for crawler========================'
current_time=`date '+%Y%m%d_%H%M%S'`
hive -e 'create external table if not exists db_crawler_temp.crawler_table_${current_time} like db_crawler_temp.crawler_table_2018_03_05;'

echo'=========== table is created=============================================='
echo'=========== start to load data============================================'

load data inpath '/user/bbdoffline/export-hbase-data/crawler-access/crawler_${current_time}/' overwrite into table db_crawler_temp.crawler_table_${current_time}; 

