# pip3 install mysql-connector-python==8.0.31

import mysql.connector
import time
import configs
import hashlib


# 当前的设计方法是直接select * ，这需要确保2端数据源的列名一致

start_time = time.time()

# 连接源端进行数据校验
source_db = mysql.connector.connect(
    host=configs.mysql_source_host,
    port=configs.mysql_source_port,
    user=configs.mysql_source_user,
    passwd=configs.mysql_source_pass,
)

source_cursor = source_db.cursor()

# 获取当前最小 最大的id，用于后续的循环比对
get_min_max_sql = (
    "SELECT min(id),max(id),count(*) from "
    + configs.mysql_source_db
    + "."
    + configs.mysql_source_tb
    + " where 1=1 ;"
)
source_cursor.execute(get_min_max_sql)
pk_range_result = source_cursor.fetchall()

for x in pk_range_result:
    min_id = x[0]
    max_id = x[1]
    count = x[2]

print(
    f"{configs.mysql_source_db}.{configs.mysql_source_tb} 最小id {min_id} , 最大id {max_id} , 总记录数 {count}"
)

start_id = min_id
stop_id = start_id + configs.step

# 记录差异行数量
diff_count = 0

while stop_id < max_id + configs.step:  # 加一个步长进去，防止因为step过大，导致有遗漏的id
    # 拼接出比对的SQL
    chksum_sql_4src = (
        "SELECT * FROM "
        + configs.mysql_source_db
        + "."
        + configs.mysql_source_tb
        + " WHERE id >="
        + str(start_id)
        + " AND id < "
        + str(stop_id)
        + " ORDER BY id ASC;"
    )
    chksum_sql_4dest = (
        "SELECT * FROM "
        + configs.mysql_dest_db
        + "."
        + configs.mysql_dest_tb
        + " WHERE id >="
        + str(start_id)
        + " AND id < "
        + str(stop_id)
        + " ORDER BY id ASC;"
    )

    source_cursor.execute(chksum_sql_4src)
    source_chksum_result = source_cursor.fetchall()

    source_chksum = dict()
    for x in source_chksum_result:
        id = x[0]
        chk_sum = hashlib.md5(str(x).replace(" ", "").encode()).hexdigest()
        source_chksum[id] = chk_sum
    # print(f"源端MySQL校验和 {source_chksum}")

    # 连接目标端进行数据校验
    dest_db = mysql.connector.connect(
        host=configs.mysql_dest_host,
        port=configs.mysql_dest_port,
        user=configs.mysql_dest_user,
        passwd=configs.mysql_dest_pass,
    )

    dest_cursor = dest_db.cursor()
    dest_cursor.execute(chksum_sql_4dest)
    dest_chksum_result = dest_cursor.fetchall()

    dest_chksum = dict()
    for x in dest_chksum_result:
        id = x[0]
        chk_sum = hashlib.md5(str(x).replace(" ", "").encode()).hexdigest()
        dest_chksum[id] = chk_sum
    # print(f"目标端MySQL校验和 {dest_chksum}")

    # 通过集合的比较，快速找出不一致的主键id
    if source_chksum != dest_chksum:
        s1 = set()
        differ = set(source_chksum.items()) ^ set(dest_chksum.items())
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

