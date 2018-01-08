#ifndef PYSPARK_EXCEPTION_H
#define PYSPARK_EXCEPTION_H

#include <Python.h>
#include <string>

class pyspark_exception
{
public:
  std::string type;
  std::string description;
  std::string traceback;
  void pyerr_fetch();

};

#endif /* PYSPARK_EXCEPTION_H  */