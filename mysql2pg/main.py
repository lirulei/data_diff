# pip3 install psycopg2==2.9.4
# pip3 install mysql-connector-python==8.0.31

import mysql.connector
import psycopg2
import time
import configs
import hashlib

# TODO 待设计
# column_list = ["id","name","ctime","utime"]  # 需要校验的列。 当前的设计方法是直接select * ，这需要确保2端数据源的列名一致


start_time = time.time()

mydb = mysql.connector.connect(
    host=configs.mysql_host,
    port=configs.mysql_port,
    user=configs.mysql_user,
    passwd=configs.mysql_pass,
)

mysql_cursor = mydb.cursor()

# 获取当前最小 最大的id，用于后续的循环比对
get_min_max_sql = (
    "SELECT min(id),max(id),count(*) from "
    + configs.mysql_db
    + "."
    + configs.mysql_tb
    + " where 1=1 ;"
)
mysql_cursor.execute(get_min_max_sql)
pk_range_result = mysql_cursor.fetchall()

for x in pk_range_result:
    min_id = x[0]
    max_id = x[1]
    count = x[2]

print(
    f"{configs.mysql_db}.{configs.mysql_tb} 最小id {min_id} , 最大id {max_id} , 总记录数 {count}"
)

start_id = min_id
stop_id = start_id + configs.step

# 记录差异行数量
diff_count = 0

while stop_id < max_id + configs.step:  # 加一个步长进去，防止因为step过大，导致有遗漏的id
    # 拼接出比对的SQL
    chksum_sql_4mysql = (
        "SELECT * FROM "
        + configs.mysql_db
        + "."
        + configs.mysql_tb
        + " WHERE id >="
        + str(start_id)
        + " AND id < "
        + str(stop_id)
        + " ORDER BY id ASC;"
    )

    chksum_sql_4pg = (
        "SELECT * FROM "
        + configs.pg_schema
        + "."
        + configs.pg_tb
        + " WHERE id >="
        + str(start_id)
        + " AND id < "
        + str(stop_id)
        + " ORDER BY id ASC;"
    )

    mysql_cursor.execute(chksum_sql_4mysql)
    mysql_chksum_result = mysql_cursor.fetchall()

    mysql_chksum = dict()
    for x in mysql_chksum_result:
        id = x[0]
        chk_sum = hashlib.md5(str(x).replace(" ", "").encode()).hexdigest()
        mysql_chksum[id] = chk_sum
    # print(f"MySQL校验和 {mysql_chksum}")

    # 连接PG进行数据校验
    pg_conn = psycopg2.connect(
        host=configs.pg_host,
        port=configs.pg_port,
        user=configs.pg_user,
        password=configs.pg_pass,
        database=configs.pg_db,
    )
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(chksum_sql_4pg)

    pg_chksum_result = pg_cursor.fetchall()

    pg_chksum = dict()
    for x in pg_chksum_result:
        id = x[0]
        chk_sum = hashlib.md5(str(x).replace(" ", "").encode()).hexdigest()
        pg_chksum[id] = chk_sum
    # print(f"PG校验和 {pg_chksum}")

    # 通过集合的比较，快速找出不一致的主键id
    if mysql_chksum != pg_chksum:
        differ = set(mysql_chksum.items()) ^ set(pg_chksum.items())
        s1 = set()
        for i in differ:
            s1.add(i[0])
        print(s1)
        with open("checksum_diff.log", "a+") as f:
            f.write(
                str(
                    s1,
                )
                + "\n"
            )

        diff_count = diff_count + len(s1)

    start_id = stop_id
    stop_id = stop_id + configs.step

stop_time = time.time()
time_dur = stop_time - start_time
print(f"比对 {count}条记录，总差异条数 {diff_count}，耗时 {time_dur} 秒")
