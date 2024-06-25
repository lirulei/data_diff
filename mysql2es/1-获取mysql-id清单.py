# -*- coding: utf-8 -*-
import logging
import random
import time
import configs
import mysql.connector
import redis
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

es = Elasticsearch(configs.es_host)

r = redis.Redis(
    host=configs.redis_host,
    port=configs.redis_port,
    password=configs.redis_pass,
    db=configs.redis_db,
    socket_connect_timeout=2,
    decode_responses=True,
)

# 定义10个队列的名称
queues = [f"{configs.redis_queue_prefix}_queue_{i}" for i in range(1, 11)]

mysql_db = mysql.connector.connect(
    host=configs.mysql_host,
    port=configs.mysql_port,
    user=configs.mysql_user,
    passwd=configs.mysql_pass,
    database=configs.mysql_db,
    ssl_disabled=True,
)

mysql_cursor = mysql_db.cursor()
mysql_dict_cursor = mysql_db.cursor(dictionary=True)  # 返回为dict类型数据结构

get_id_sql = f"{configs.mysql_id_sql}"
mysql_cursor.execute(get_id_sql)
res2 = mysql_cursor.fetchall()

# 使用pipeline提高效率
pipe = r.pipeline()
start_time = time.time()

for i in res2:
    queue = random.choice(queues)  # 随机选择一个队列
    pipe.rpush(queue, i[0])

pipe.execute()
end_time = time.time()
print(f"批量插入到Redis队列耗时: {end_time - start_time}秒")
