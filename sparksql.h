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
typedef map<string, string> MAP_ALIA_SQL;
class sparksql
{
public:
  string url;
  string user;
  string password;
  MAP_ALIA_SQL dbtable;
  string sql;
  string dst_table;
  string spark_master;

  void run();
  void pyinit();
  void pyfinal();
  //void clear();
  sparksql();
  ~sparksql();

private:
  PyObject *py_module;
  PyObject *py_moduledict;
  PyObject *py_class_sparksql;


  void setValueToPython();
  void checkValue();
};
}

#endif /* DBSCALE_SPARKSQL_H */