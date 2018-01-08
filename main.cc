#include "sparksql.h"
#include <iostream>
#include <sstream>
#include <ctime>
using namespace std;
using namespace dbscale;

void run();
void run1();
void run2();
string gettime();

int main()
{
  run();
  for (int i = 0; i < 10; i++)
  {
    run2();
  }

  return 0;
}

void run()
{
  cout << "1" << endl;
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

  ss.dst_table = "target10000";
  ss.spark_master = "local[4]";
  ss.run();
}

void run1()
{
  cout << "1" << endl;
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

  ss.dst_table = gettime();
  ss.spark_master = "local[2]";
  ss.run();
}

void run2()
{
  cout << "2" << endl;
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

  ss.dst_table = gettime();
  ss.spark_master = "local[2]";
  ss.run();

  ss.pyfinal();
}

string gettime()
{
  time_t result = time(nullptr);
  stringstream ss;
  ss << "test";
  ss << result;
  string s;
  ss >> s;
  return s;
}
