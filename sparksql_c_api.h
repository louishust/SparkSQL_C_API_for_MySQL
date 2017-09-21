#ifndef SYD_SPARKSQL_C_API_H_
#define SYD_SPARKSQL_C_API_H_

#include <vector>
#include <string>
using namespace std;

typedef vector<string> VectorString;

namespace sparksql
{

void run_sql(
    vector<VectorString> _url_user_password_vector_vector,
    VectorString _tables_vector,
    string _sql_string,
    string _target_name_string,
    VectorString _target_vector = VectorString(),
    string _master_string = "local",
    string _appname_string = "run_sql",
    VectorString _tables_alias_vector = VectorString());
/*
    run sql on spark and return a table in MySQL as result
    ------------------------------------------------------
    _url_user_password_vector_vector:
        ((url, user, password), (url, user, password),...,(...))
        a vector of vectors which contains urls, users and passwords
        len(_url_user_password_vector_vector) == 1 or
        len(_url_user_password_vector_vector) == len(_tables_vector)

    _tables_vector:
        (table1, table2, table3, .... , tableN)
        tables which the sql will use
        table can be a table_name in MySQL or a selet statement
        when you use select statements as tables,
        you should use _tables_alia_vector param

    _sql_string:
        a sql statement you will run on spark

    _target_vector:
        (target_url, target_user, target_password)
        a table will store the result of the sql
        if _target_vector is None:
            _target_vector = _url_user_password_vector_vector[0]

    _master_string="local":
        a string to config the spark master
        to know more about this param, you should read spark's document

    _appname_string="run_sql":
        a name will show on the web of spark

    _tables_alias_vector=None:
        (table_alias1, table_alias2, table_alias3, .... , table_aliasN)
        tables' aliases
    */
}

#endif
