#ifndef DBSCALE_SPARKSQL_H
#define DBSCALE_SPARKSQL_H

#include <map>
#include <Python.h>
using namespace std;

/*
 * Copyright (C) 2017 Great OpenSource Inc. All Rights Reserved.
 */

namespace dbscale
{
class sparksql
{
public:
    string url;
    string user;
    string password;
    map<string, string> dbtable;
    string sql;
    string dst_table;
    string spark_master;
    PyObject *py_module;
    PyObject *py_class;
    PyObject *py_moduledict;
    void run();
    sparksql();
    ~sparksql();

private:
    void setValueToPython();
};
}

#endif /* DBSCALE_SPARKSQL_H */