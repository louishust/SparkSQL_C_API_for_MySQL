#! /usr/bin/env python
"""
    c/c++ api: to run sql statements on SparkSQL
"""
from pyspark.sql import SparkSession

# master[number_of_threads]
# spark://ip:port or local


class DBScaleSparksql(object):
    """
        sparksql for dbscale
    """
    url = ""
    user = ""
    password = ""
    dbtable = {}# {T1:Q1, T2:Q2, ... }
    sql = ""
    dst_table = ""
    spark_master = ""

    @staticmethod
    def get_url(_url):
        """
            get_url
        """
        DBScaleSparksql.url = _url
        return 1

    @staticmethod
    def get_user(_user):
        """
            get_user
        """
        DBScaleSparksql.user = _user
        return 1

    @staticmethod
    def get_password(_password):
        """
            get_password
        """
        DBScaleSparksql.password = _password
        return 1

    @staticmethod
    def get_dbtable(_dbtable):
        """
            get_dbtable
        """
        DBScaleSparksql.dbtable = _dbtable
        return 1

    @staticmethod
    def get_sql(_sql):
        """
            get_sql
        """
        DBScaleSparksql.sql = _sql
        return 1

    @staticmethod
    def get_dst_table(_dst_table):
        """
            get_dst_table
        """
        DBScaleSparksql.dst_table = _dst_table
        return 1

    @staticmethod
    def get_spark_master(_spark_master):
        """
            get_spark_master
        """
        DBScaleSparksql.spark_master = _spark_master
        return 1

    @staticmethod
    def print_all():
        """
            print all things to debug
            url = ""
            user = ""
            password = ""
            dbtable = {}
            sql = ""
            dst_table = ""
            spark_master = ""
        """
        print DBScaleSparksql.url
        print DBScaleSparksql.user
        print DBScaleSparksql.password
        print DBScaleSparksql.dbtable
        print DBScaleSparksql.sql
        print DBScaleSparksql.dst_table
        print DBScaleSparksql.spark_master
        return 1

    @staticmethod
    def run():
        """
            run sql on spark cluster
        """
        spark_session = SparkSession.builder.master(DBScaleSparksql.spark_master) \
            .appName("DBScale") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        spark_session = spark_session.newSession()
        for tablename, query in DBScaleSparksql.dbtable.items():
            table = spark_session.read.format("jdbc").options(
                url=DBScaleSparksql.url,
                driver="com.mysql.jdbc.Driver",
                dbtable=query,
                user=DBScaleSparksql.user,
                password=DBScaleSparksql.password
            ).load()
            table.createOrReplaceTempView(tablename)
        table = spark_session.sql(DBScaleSparksql.sql)
        table.write.format("jdbc").options(
            url=DBScaleSparksql.url,
            driver="com.mysql.jdbc.Driver",
            dbtable=DBScaleSparksql.dst_table,
            user=DBScaleSparksql.user,
            password=DBScaleSparksql.password
        ).save()
        return 1


if __name__ == "__main__":
    """
        url = ""
        user = ""
        password = ""
        dbtable = {}
        sql = ""
        dst_table = ""
        spark_master = ""
    """
    DBSS = DBScaleSparksql()
    DBSS.get_url("jdbc:mysql://localhost:3306/test")
    DBSS.get_user("root")
    DBSS.get_password("abc123")
    DBSS.get_dbtable({"select * from pet1": "pet1",
                      "select * from pet2": "pet2",
                      "select * from pet3": "pet3"})
    DBSS.get_sql(
        "select * from pet1 union select * from pet2 union select * from pet3")
    DBSS.get_dst_table("target")
    DBSS.get_spark_master("local[2]")
    DBSS.print_all()
    #DBSS.run()
