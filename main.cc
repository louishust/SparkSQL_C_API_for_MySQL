#include "sparksql.h"
#include <iostream>
using namespace std;
using namespace dbscale;
int main()
{
  sparksql ss;
  ss.url = "jdbc:mysql://localhost:3306/test";
  ss.user = "root";
  ss.password = "abc123";
  MAP_ALIA_SQL dbt;
  dbt.insert(pair<string, string>("pet1", "select * from pet1"));
  dbt.insert(pair<string, string>("pet2", "select * from pet2"));
  dbt.insert(pair<string, string>("pet3", "select * from pet3"));
  ss.dbtable = dbt;
  ss.sql =
      "select * from pet1 union select * from pet2 union select * from pet3";
  ss.dst_table = "target";
  ss.spark_master = "local[2]";
  ss.run();
  return 0;
}
