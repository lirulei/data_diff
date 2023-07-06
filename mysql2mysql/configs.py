# MySQL数据源的信息
mysql_source_host = "192.168.27.183"
mysql_source_port = "3306"
mysql_source_user = "dts"
mysql_source_pass = "dts"
mysql_source_db = "sbtest"
mysql_source_tb = "tb1"

# MySQL目标库的信息
mysql_dest_host = "127.0.0.1"
mysql_dest_port = "3316"
mysql_dest_user = "dts"
mysql_dest_pass = "dts"
mysql_dest_db = "sbtest"
mysql_dest_tb = "tb1"


# 每次遍历的记录数
step = 1000  # 步长。 1表示逐条检测。这个值建议在1000-2000之间。
