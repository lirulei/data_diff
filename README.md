# data_diff
用于比对mysql和mysql/pg之间的数据差异，前提是需要确保二者的列的顺序是一致的。

## 说明
- mysql2mysql 用于源端和目标端都是MySQL的数据比对场景。
- mysql2pg 用于源端是MySQL，目标端是PG的数据比对场景。

## 依赖
### mysql2mysql
- pip3 install psycopg2==2.9.4
### mysql2pg
- pip3 install psycopg2==2.9.4
- pip3 install mysql-connector-python==8.0.31

## 性能
- step为1000时，每秒大约可以比对2.5k条记录
