#! /usr/bin/python
"""
    c/c++ api: to run sql statements on SparkSQL
"""
from pyspark.sql import SparkSession


def run_sql(_url,
            _user,
            _password,
            _tables,
            _sql,
            _target_name,
            _target_rul=None,
            _target_user=None,
            _target_password=None,
            _master="local",
            _appname="a",
            _tables_alia=None):
    """
        master
        url
        user
        password
        tables
        sql
    """
    if _tables_alia is None:
        _tables_alia = _tables
    elif len(_tables) != len(_tables_alia):
        print "len(tables) != len(tables_alia)"
        return
    spark_session = SparkSession.builder.master(_master) \
        .appName(_appname) \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark_session = spark_session.newSession()
    for i in xrange(len(_tables)):
        table = spark_session.read.format("jdbc").options(
            url=_url,
            driver="com.mysql.jdbc.Driver",
            dbtable=_tables[i],
            user=_user,
            password=_password
        ).load()
        table.createOrReplaceTempView(_tables_alia[i])
    table = spark_session.sql(_sql)
    if _target_rul is None:
        _target_rul = _url
        _target_user = _user
        _target_password = _password
    table.write.format("jdbc").options(
        url=_target_rul,
        driver="com.mysql.jdbc.Driver",
        dbtable=_target_name,
        user=_target_user,
        password=_target_password
    ).save()


if __name__ == "__main__":
    run_sql("jdbc:mysql://localhost:3306/test",
            "root",
            "123",
            ("pet1", "pet2", "pet3"),
            "select * from pet1 union select * from pet2 union select * from pet3",
            "target")
