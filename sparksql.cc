/*
 * Copyright (C) 2017 Great OpenSource Inc. All Rights Reserved.
 */

#include "sparksql.h"
#include <Python.h>
#include <iostream>

using namespace dbscale;

sparksql::sparksql()
{
  Py_Initialize();
  if (!Py_IsInitialized())
  {
    cout << "Error: Py_IsInitialized(): false" << endl;
    exit(-1);
  }
  PyRun_SimpleString("import sys");
  // path to sparksql.py
  PyRun_SimpleString("sys.path.append('../')");

  this->py_module = PyImport_ImportModule("sparksql");
  if (!this->py_module)
  {
    cout << "Error: Sparksql.py is not imported" << endl;
    exit(-1);
  }

  this->py_moduledict = PyModule_GetDict(this->py_module);
  if (!this->py_moduledict)
  {
    cout << "Error: can't find dict of python module" << endl;
    exit(-1);
  }

  string pycla_sparksql = "DBScaleSparksql";
  this->py_class_sparksql = PyDict_GetItemString(
      this->py_moduledict,
      pycla_sparksql.c_str());
  if (!this->py_class_sparksql)
  {
    cout << "Error: can't find class called DBScaleSparksql" << endl;
    exit(-1);
  }
}

sparksql::~sparksql()
{
  Py_DECREF(this->py_module);
  Py_DECREF(this->py_moduledict);
  Py_DECREF(this->py_class_sparksql);

  Py_Finalize();
}

void sparksql::run()
{
  cout << "hello" << endl;
  this->setValueToPython();
  
}

void sparksql::setValueToPython()
{
  string py_meth_geturl = "get_url";
  string py_meth_getuser = "get_user";
  string py_meth_getpassword = "get_password";
  string py_meth_getdbtable = "get_dbtable";
  string py_meth_getsql = "get_sql";
  string py_meth_getdsttable = "get_dst_table";
  string py_meth_getsparkmaster = "get_spark_master";
  string py_meth_printall = "print_all";
  PyObject *pysparkmaster = PyString_FromString(this->spark_master.c_str());
  // cout << endl;
  // PyObject_CallMethodObjArgs(
  //     py_class_sparksql,
  //     PyString_FromString(py_meth_printall.c_str()),
  //     NULL);
  // cout << endl;
  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_geturl.c_str()),
      PyString_FromString(this->url.c_str()),
      NULL);
  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_getuser.c_str()),
      PyString_FromString(this->user.c_str()),
      NULL);
  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_getpassword.c_str()),
      PyString_FromString(this->password.c_str()),
      NULL);
  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_getsql.c_str()),
      PyString_FromString(this->sql.c_str()),
      NULL);
  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_getdsttable.c_str()),
      PyString_FromString(this->dst_table.c_str()),
      NULL);
  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_getsparkmaster.c_str()),
      PyString_FromString(this->spark_master.c_str()),
      NULL);

  PyObject *pydbtable = PyDict_New();
  for (MAP_ALIA_SQL::iterator iter = this->dbtable.begin();
       iter != this->dbtable.end();
       iter++)
  {
    // cout << iter->first << " " << iter->second << endl;
    PyDict_SetItem(pydbtable,
                   PyString_FromString(iter->first.c_str()),
                   PyString_FromString(iter->second.c_str()));
  }

  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_getdbtable.c_str()),
      pydbtable,
      NULL);

  PyObject_CallMethodObjArgs(
      py_class_sparksql,
      PyString_FromString(py_meth_printall.c_str()),
      NULL);
  Py_DECREF(pysparkmaster);
  Py_DECREF(pydbtable);
  cout << "set values to sparksql.py" << endl;
}