# -*- coding: utf-8 -*-

# MySQL连接信息
mysql_host = "192.168.3.14"
mysql_port = "3306"
mysql_user = "dts"
mysql_pass = "dts"
mysql_db = "sbtest"

# ES连接信息
es_host = ["192.168.3.14:9200"]
es_index = "tb1"

# Redis连接信息
redis_host = "192.168.3.14"
redis_port = 6379
redis_pass = ""
redis_db = "0"
redis_queue_prefix = "sbtest1"
redis_retry_queue = "sbtest1_retry_queue"

# 根据where条件，取出MySQL的主键id，生产上通常是where update_time>xxxx这种增量写法
mysql_id_sql = "select id from core_query_records where time >DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d %H:%i:%s') order by id asc"

# 在 step2 脚本里面，会对这个sql后面拼接上主键id进行keypoint查询
mysql_full_column_sql = "select id as esId,work_id,`sql`,ex_time,`time`,`source`,base_name from core_query_records"
# 注意这里的es_column_list需要和上面mysql_full_column_sql里面的列对的上，都存在就行，对列的顺序没有特殊要求
es_column_list = ["esId", "work_id", "sql", "ex_time", "time", "source", "base_name"]
