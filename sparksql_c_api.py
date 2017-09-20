#! /usr/bin/env python
"""
    c/c++ api: to run sql statements on SparkSQL
"""
from pyspark.sql import SparkSession


def run_sql(_url_user_password_tuple_tuple,
            _tables_tuple,
            _sql_string,
            _target_name_string,
            _target_tuple=None,
            _master_string="local",
            _appname_string="run_sql",
            _tables_alias_tuple=None):
    """
        run sql on spark and return a table in MySQL as result
        ------------------------------------------------------
        _url_user_password_tuple_tuple:
            ((url, user, password), (url, user, password),...,(...))
            a tuple of tuples which contains urls, users and passwords
            len(_url_user_password_tuple_tuple) == 1 or
            len(_url_user_password_tuple_tuple) == len(_tables_tuple)

        _tables_tuple:
            (table1, table2, table3, .... , tableN)
            tables which the sql will use
            table can be a table_name in MySQL or a selet statement
            when you use select statements as tables,
            you should use _tables_alia_tuple param

        _sql_string:
            a sql statement you will run on spark

        _target_tuple:
            (target_url, target_user, target_password)
            a table will store the result of the sql
            if _target_tuple is None:
                _target_tuple = _url_user_password_tuple_tuple[0]

        _master_string="local":
            a string to config the spark master
            to know more about this param, you should read spark's document

        _appname_string="run_sql":
            a name will show on the web of spark

        _tables_alias_tuple=None:
            (table_alias1, table_alias2, table_alias3, .... , table_aliasN)
            tables' aliases
    """
    if _tables_alias_tuple is None:
        _tables_alia_tuple = _tables_tuple
    elif len(_tables_tuple) != len(_tables_alias_tuple):
        print "len(tables) != len(tables_alia)"
        return

    uuptt = ()
    if len(_url_user_password_tuple_tuple) == 1:
        for i in xrange(len(_tables_tuple)):
            uuptt += _url_user_password_tuple_tuple
    else:
        uuptt += _url_user_password_tuple_tuple

    spark_session = SparkSession.builder.master(_master_string) \
        .appName(_appname_string) \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark_session = spark_session.newSession()
    for i in xrange(len(_tables_tuple)):
        table = spark_session.read.format("jdbc").options(
            url=uuptt[i][0],
            driver="com.mysql.jdbc.Driver",
            dbtable=_tables_tuple[i],
            user=uuptt[i][1],
            password=uuptt[i][1]
        ).load()
        table.createOrReplaceTempView(_tables_alia_tuple[i])
    table = spark_session.sql(_sql_string)
    if _target_tuple is None:
        _target_tuple = uuptt[0]
    table.write.format("jdbc").options(
        url=_target_tuple[0],
        driver="com.mysql.jdbc.Driver",
        dbtable=_target_name_string,
        user=_target_tuple[1],
        password=_target_tuple[2]
    ).save()


if __name__ == "__main__":
    run_sql("jdbc:mysql://localhost:3306/test",
            "root",
            "123",
            ("pet1", "pet2", "pet3"),
            "select * from pet1 union select * from pet2 union select * from pet3",
            "target")
