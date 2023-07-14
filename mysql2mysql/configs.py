# MySQL数据源的信息
mysql_source_host = "192.168.3.14"
mysql_source_port = "3306"
mysql_source_user = "dts"
mysql_source_pass = "dts"
mysql_source_db = "sbtest"
mysql_source_tb = "tb1"

# 源端的where条件
source_condition = "pad LIKE '%39216%' OR k IN (4708,5038,4996)"

# MySQL目标库的信息
mysql_dest_host = "192.168.3.14"
mysql_dest_port = "3306"
mysql_dest_user = "dts"
mysql_dest_pass = "dts"
mysql_dest_db = "sbtest"
mysql_dest_tb = "tb2"

# 每次遍历的记录数
step = 1000  # 步长。 1表示逐条检测。这个值建议在1000-2000之间。

# 每轮休眠的时间(单位秒,默认0.1秒)
sleep_time = 0.1

# 需要比对的列。如果是ALL则自动以src端的列顺序为准。如果是指定的列(英文逗号分隔开) 则以这里指定的列为准
column_flag = "ALL"
# column_flag = "k,c,pad"
