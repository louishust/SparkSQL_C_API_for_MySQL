#! /usr/bin/env python
from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf

# spark_conf = SparkConf.setAppName('DBScale')
# spark_conf.set('spark.sql.crossJoin.enabled', 'true')

spark_session = SparkSession.builder.master("local[2]") \
    .appName("DBScale") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .getOrCreate()
spark_session = spark_session.newSession()

dict_sql = {
    'dbscale_t_r_test_norm_table2_dbscale_t_r': 
    '(select c1 from test.n1 where 1=0  OR (1=1  AND 1=1 ))dbscale_t_r_test_norm_table2_dbscale_t_r',
    'dbscale_t_r_test_part_table2_dbscale_t_r': 
    '(select c2 from test.n2 where 1=0  OR (1=1  AND 1=1 ))dbscale_t_r_test_part_table2_dbscale_t_r',
    'dbscale_t_r_test_norm_table1_dbscale_t_r': 
    '(select c5 from test.p2 where 1=0  OR (1=1  AND 1=1 ))dbscale_t_r_test_norm_table1_dbscale_t_r'
}
for tablename, query in dict_sql.items():
    table = spark_session.read.format("jdbc").options(
        url='jdbc:mysql://localhost:3306/test',
        driver="com.mysql.jdbc.Driver",
        dbtable=query,
        user='root',
        password='abc123'
    ).load()
    table.createOrReplaceTempView(tablename)
    table.show()
#table = spark_session.sql('select p2.c5, n1.c1, n2.c2 from p2 cross join n1 cross join n2') # ok
table.show()
table = spark_session.sql(
    'select dbscale_t_r_test_part_table2_dbscale_t_r.c5,dbscale_t_r_test_norm_table2_dbscale_t_r.c1,dbscale_t_r_test_norm_table1_dbscale_t_r.c2 fromdbscale_t_r_test_part_table2_dbscale_t_r,dbscale_t_r_test_norm_table2_dbscale_t_r,dbscale_t_r_test_norm_table1_dbscale_t_r')  # error
#table = spark_session.sql('select p2.c5, n1.c1 from p2, n1') # error
# table = spark_session.sql(
#     'select p2.c5 from p2 join (select n1.c1 from n1)')
table.show()
