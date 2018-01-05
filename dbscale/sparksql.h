#ifndef DBSCALE_SPARKSQL_H
#define DBSCALE_SPARKSQL_H

#include <map>
#include <string>
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
  sparksql();
  ~sparksql();
  void clear();
  int initialize();

private:
  void *py_module;
  void *py_moduledict;
  void *py_class_sparksql;


  void setValueToPython();
  void checkValue();
};
}

#endif /* DBSCALE_SPARKSQL_H */
