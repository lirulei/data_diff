# MySQL数据源的信息
mysql_host = "localhost"
mysql_port = "3306"
mysql_user = "dts"
mysql_pass = "dts"
mysql_db = "sbtest"
mysql_tb = "tb1"

# PostgreSQL目标库的信息
pg_host = "localhost"
pg_port = "5432"
pg_user = "dts"
pg_pass = "dts"
pg_db = "postgres"
pg_schema = "public"
pg_tb = "tb1"


# 每次遍历的记录数
step = 1000  # 步长。 1表示逐条检测。这个值建议在1000-2000之间。
