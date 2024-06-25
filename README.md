# data_diff
用于比对mysql和mysql/pg/es之间的数据差异，mysql2mysql和mysql2pg需要确保二者的列的顺序是一致的，mysql2es二者的列顺序无所谓。

## 说明
- mysql2mysql 用于源端和目标端都是MySQL的数据比对场景。
- mysql2pg 用于源端是MySQL，目标端是PG的数据比对场景。
- 特别注意：mysql2mysql和mysql2pg这2个工具只支持主键为整型单调递增。代码里写死了主键为id，如果主键非id的话，批量替换下即可。非自增主键的场景，目前脚本还不支持。
- mysql2es 用于源端是MySQL，目标端是ES的数据比对场景。它会将差异的es id输出到redis queue中。

## 依赖
### mysql2mysql
- pip3 install psycopg2==2.9.4
### mysql2pg
- pip3 install psycopg2==2.9.4
- pip3 install mysql-connector-python==8.0.31
### mysql2es
- pip3 install elasticsearch==7.13.1
- pip3 install mysql-connector-python==8.0.31
- pip3 install redis==3.5.3
## 性能
- step为1000时，每秒大约可以比对2.5k条记录

## 不足
- mysql2pg的脚本，不能处理bit类型，会出现误判的情况
- mysql2pg的脚本，暂时不具备where条件的功能，待完善
