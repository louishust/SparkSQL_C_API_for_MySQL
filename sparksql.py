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
    dbtable = {}
    sql = ""
    dst_table = ""
    spark_master = ""

    def get_url(self, _url):
        """
            get_url
        """
        self.url = _url

    def get_user(self, _user):
        """
            get_user
        """
        self.user = _user

    def get_password(self, _password):
        """
            get_password
        """
        self.password = _password

    def get_dbtable(self, _dbtable):
        """
            get_dbtable
        """
        self.dbtable = _dbtable

    def get_sql(self, _sql):
        """
            get_sql
        """
        self.sql = _sql

    def get_dst_table(self, _dst_table):
        """
            get_dst_table
        """
        self.dst_table = _dst_table

    def get_spark_master(self, _spark_master):
        """
            get_spark_master
        """
        self.spark_master = _spark_master

    def print_all(self):
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
        print self.url
        print self.user
        print self.password
        print self.dbtable
        print self.sql
        print self.dst_table
        print self.spark_master

    def run(self):
        """
            run sql on spark cluster
        """
        spark_session = SparkSession.builder.master(self.spark_master) \
            .appName("DBScale") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        spark_session = spark_session.newSession()
        for query, tablename in self.dbtable.items():
            table = spark_session.read.format("jdbc").options(
                url=self.url,
                driver="com.mysql.jdbc.Driver",
                dbtable=query,
                user=self.user,
                password=self.password
            ).load()
            table.createOrReplaceTempView(tablename)
        table = spark_session.sql(self.sql)
        table.write.format("jdbc").options(
            url=self.url,
            driver="com.mysql.jdbc.Driver",
            dbtable=self.dst_table,
            user=self.user,
            password=self.password
        ).save()


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
    # DBSS.run()
