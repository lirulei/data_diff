# -*- coding: utf-8 -*-

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import configs
import mysql.connector
import redis
from elasticsearch import Elasticsearch

# logging.basicConfig(level=logging.WARN)
logging.basicConfig(
    level=logging.ERROR,
    filename="run.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def process_item(queue_name):
    es = Elasticsearch(configs.es_host)

    r = redis.Redis(
        host=configs.redis_host,
        port=configs.redis_port,
        password=configs.redis_pass,
        db=configs.redis_db,
        socket_connect_timeout=2,
        decode_responses=True,
    )

    mysql_db = mysql.connector.connect(
        host=configs.mysql_host,
        port=configs.mysql_port,
        user=configs.mysql_user,
        passwd=configs.mysql_pass,
        database=configs.mysql_db,
        ssl_disabled=True,
    )
    mysql_dict_cursor = mysql_db.cursor(dictionary=True)  # 返回为dict类型数据结构

    while True:
        if r.llen(queue_name) <= 0:
            break

        doc_id = r.lpop(queue_name)
        try:
            es_data = es.get(
                index=configs.es_index, id=doc_id, _source=configs.es_column_list
            )
            es_content = es_data["_source"]

            mysql_dict_cursor.execute(
                f"{configs.mysql_full_column_sql} where id={doc_id}"
            )

            mysql_content = mysql_dict_cursor.fetchone()

            # print("es_content --> ", es_content)
            # print("mysql_content --> ", mysql_content)

            if es_content == mysql_content:
                logger.info(f"doc_id的记录： {doc_id} 二者结果相同")
            else:
                logger.error(
                    f"doc_id的记录： {doc_id} 二者结果不同，或者在es中不存在该条记录，已将id记录到redis中"
                )
                r.lpush(configs.redis_retry_queue, doc_id)
        except Exception as e:
            logger.error(f"doc_id的记录： {doc_id} 处理时发生错误: {e}，已将id记录到redis中")
            r.lpush(configs.redis_retry_queue, doc_id)


if __name__ == "__main__":
    start_time = time.time()

    try:
        executor = ThreadPoolExecutor(max_workers=10)
        queue_list = [f"{configs.redis_queue_prefix}_queue_{i}" for i in range(1, 11)]
        all_task = [executor.submit(process_item, (queue)) for queue in queue_list]

        for future in as_completed(all_task):
            data = future.result()
            print("in main: ID {} success".format(data))
    except Exception as e:
        print(str(e))

    # 最后输出下redis 队列的长度
    r = redis.Redis(
        host=configs.redis_host,
        port=configs.redis_port,
        password=configs.redis_pass,
        db=configs.redis_db,
        socket_connect_timeout=2,
        decode_responses=True,
    )
    queue_len = r.llen(configs.redis_retry_queue)
    print("检测到出现异常的item数 --> ", queue_len)

    end_time = time.time()
    print(f"比对耗时: {end_time - start_time}秒")


"""
性能测试：
900条记录（10并发处理）
step1 耗时 0.02秒
step2 耗时 1.38秒

日志类似如下：
2024-06-25 09:43:08,127 - ERROR: doc_id的记录： 1 处理时发生错误: NotFoundError(404, '{"_index":"tb1","_type":"_doc","_id":"1","found":false}')，已将id记录到redis中
2024-06-25 09:43:08,139 - ERROR: doc_id的记录： 4 处理时发生错误: NotFoundError(404, '{"_index":"tb1","_type":"_doc","_id":"4","found":false}')，已将id记录到redis中
2024-06-25 09:43:08,140 - ERROR: doc_id的记录： 3 处理时发生错误: NotFoundError(404, '{"_index":"tb1","_type":"_doc","_id":"3","found":false}')，已将id记录到redis中
2024-06-25 09:43:08,143 - ERROR: doc_id的记录： 2 处理时发生错误: NotFoundError(404, '{"_index":"tb1","_type":"_doc","_id":"2","found":false}')，已将id记录到redis中
"""
