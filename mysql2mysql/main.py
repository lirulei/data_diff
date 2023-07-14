# pip3 install mysql-connector-python==8.0.31

import mysql.connector
import time
import configs
import hashlib


start_time = time.time()
s1 = set()

filename = 'fixed_' + time.time() + '.sql'

# 连接源端进行数据校验
source_db = mysql.connector.connect(
    host=configs.mysql_source_host,
    port=configs.mysql_source_port,
    user=configs.mysql_source_user,
    passwd=configs.mysql_source_pass,
)
source_cursor = source_db.cursor()


# 从SRC源端获取列名请单，如果column_flag是ALL表示以src端的列为准，否则以配置文件中指定的列为准
if configs.column_flag == "ALL":
    get_coulmns = (
        "SELECT GROUP_CONCAT(COLUMN_NAME order by ORDINAL_POSITION ASC) AS cname from information_schema.columns where table_schema='"
        + configs.mysql_source_db +"' AND table_name='" +  configs.mysql_source_tb + "' ORDER BY ORDINAL_POSITION ASC; "
    )
    source_cursor.execute("set group_concat_max_len=10240;")
    source_cursor.execute(get_coulmns)
    column_list = source_cursor.fetchone()[0]
else:
    column_list = configs.column_flag

print("--- 待校验的列清单 ---",column_list)

# 连接SRC源端 获取当前最小 最大的id，用于后续的循环比对
get_min_max_sql = (
    "SELECT min(id),max(id) from "
    + configs.mysql_source_db
    + "."
    + configs.mysql_source_tb
    + ";"
)

source_cursor.execute(get_min_max_sql)
pk_range_result = source_cursor.fetchall()

for x in pk_range_result:
    min_id = x[0]
    max_id = x[1]

print(
    f"SRC源端 {configs.mysql_source_db}.{configs.mysql_source_tb} ：最小id {min_id} , 最大id {max_id}"
)





start_id = min_id
stop_id = start_id + configs.step


while stop_id < max_id + configs.step:  # 加一个步长进去，防止因为step过大，导致有遗漏的id

    # 加个sleep时间，避免对数据库造成压力
    time.sleep(configs.sleep_time)

    # 拼接出比对的SQL
    chksum_sql_4src = (
        "SELECT " + column_list + " FROM "
        + configs.mysql_source_db
        + "."
        + configs.mysql_source_tb
        + " FORCE INDEX (PRIMARY) WHERE 1=1 AND ( " + configs.source_condition + " ) AND id >="
        + str(start_id)
        + " AND id < "
        + str(stop_id)
        + " ORDER BY id ASC;"
    )

    # print('src查询语句 --> ',chksum_sql_4src)

    chksum_sql_4dest = (
        "SELECT " + column_list + " FROM "
        + configs.mysql_dest_db
        + "."
        + configs.mysql_dest_tb
        + " WHERE id >="
        + str(start_id)
        + " AND id < "
        + str(stop_id)
        + " ORDER BY id ASC;"
    )
    # print('dest查询语句 --> ',chksum_sql_4dest)


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
        differ = set(source_chksum.items()) ^ set(dest_chksum.items())
        for i in differ:
            s1.add(i[0])
        print("差异的id列表: ",s1)
        with open("checksum_diff.log", "w+") as f:
            f.write(
                str(
                    s1,
                )
                + "\n"
            )

    start_id = stop_id
    stop_id = stop_id + configs.step
    print("当前处理的id范围: ",start_id,"~",stop_id)

stop_time = time.time()
time_dur = stop_time - start_time
print(f"总差异条数 {len(s1)}，耗时 {time_dur} 秒")


if len(s1) ==0:
    pass
else:
    print("---------------- 开始生成修复sql到文件(对bit类型暂不支持，需要人工fix下数据) ------------------\n")
    source_db = mysql.connector.connect(
        host=configs.mysql_source_host,
        port=configs.mysql_source_port,
        user=configs.mysql_source_user,
        passwd=configs.mysql_source_pass,
    )
    source_cursor = source_db.cursor()

    for id in sorted(s1):
        get_fixed_sql = (
                "SELECT " + column_list + " FROM "
                + configs.mysql_source_db
                + "."
                + configs.mysql_source_tb
                + " WHERE 1=1 AND id=" + str(id) +";"
        )
        source_cursor.execute(get_fixed_sql)
        res = source_cursor.fetchall()

        for row in res:
            value_list = []
            for col in row:
                value_list.append(str(col))
            raw_sql = "REPLACE INTO `" + configs.mysql_dest_db +"`.`" +configs.mysql_dest_tb  + "` (" + str(column_list) + ") VALUES " + str(tuple(value_list)) + ";"
            format_raw_sql = raw_sql.replace("'None'",'NULL')
            # print(format_raw_sql)
            with open(filename,"a+") as f:
                f.writelines(format_raw_sql+"\n")



"""
脚本运行后，如果有需要修复的数据，则会生成2个文件
checksum_diff.log 记录了差异的id清单
fixed_1689145280.sql 记录了用于修复数据的sql明细（对bit类型支持有问题，还没找到修复方法）


"""
