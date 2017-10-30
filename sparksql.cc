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
  PyRun_SimpleString("sys.path.append('./')");
  this.py_module = PyImport_ImportModule("sparksql");
  if (!this.py_module)
  {
    cout << "Error: Sparksql.py is not imported" << endl;
    exit(-1);
  }
  this.py_moduledict = PyModule_GetDict(this.py_module);
  if (!this.py_moduledict)
  {
    cout << "Error: can't find dict of python module" << endl;
    exit(-1);
  }
}

sparksql::~sparksql()
{
  Py_Finalize();
}

void sparksql::run()
{
  cout << "hello" << endl;
  this.setValueToPython();
}

void sparksql::setValueToPython()
{
  cout << "python" << endl;
}