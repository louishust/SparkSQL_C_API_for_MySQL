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

  void run();
  sparksql();
  ~sparksql();

private:
  PyObject *py_module;
  PyObject *py_moduledict;
  PyObject *py_class_sparksql;
  void setValueToPython();
};
}

#endif /* DBSCALE_SPARKSQL_H */