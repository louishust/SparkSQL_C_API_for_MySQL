/*
 * Copyright (C) 2017 Great OpenSource Inc. All Rights Reserved.
 */

#include <Python.h>
#include "sparksql.h"
#include <iostream>
#include <statement.h>
#include "option.h"

using namespace dbscale;

sparksql::sparksql()
{
}

int sparksql::initialize()
{
    Py_Initialize();
    if (!Py_IsInitialized())
    {
        cout << "Error: Py_IsInitialized(): false" << endl;
        return 1;
    }
    PyRun_SimpleString("import sys");
    // path to sparksql.py
    string pyspark_path = "sys.path.append('";
    pyspark_path.append(py_sparksql_location);
    pyspark_path.append("')");
    PyRun_SimpleString(pyspark_path.c_str());

    this->py_module = (void*) PyImport_ImportModule("sparksql");
    if (!this->py_module)
    {
        cout << "Error: Sparksql.py is not imported" << endl;
        return 1;
    }

    this->py_moduledict = (void*) PyModule_GetDict((PyObject*) this->py_module);
    if (!this->py_moduledict)
    {
        cout << "Error: can't find dict of python module" << endl;
        return 1;
    }

    string pycla_sparksql = "DBScaleSparksql";
    this->py_class_sparksql = (void*) PyDict_GetItemString(
        (PyObject* )this->py_moduledict,
        pycla_sparksql.c_str());
    if (!this->py_class_sparksql)
    {
        cout << "Error: can't find class called DBScaleSparksql" << endl;
        return 1;
    }
    return 0;
}

sparksql::~sparksql()
{
 // Py_DECREF(this->py_module);
 // Py_DECREF(this->py_moduledict);
 // Py_DECREF(this->py_class_sparksql);
}

void sparksql::clear()
{
  //  Py_Finalize();
}

void sparksql::run()
{
    this->checkValue();
    cout << "runing" << endl;
    this->setValueToPython();
    string py_meth_run = "run";
    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_run.c_str()),
        NULL);
    cout << "complete" << endl;
}

void sparksql::checkValue()
{
    if (this->url.empty())
    {
        cout << "Warning: url is not set" << endl;
    }
    if (this->user.empty())
    {
        cout << "Warning: user is not set" << endl;
    }
    if (this->password.empty())
    {
        cout << "Warning: password is not set" << endl;
    }
    if (this->dbtable.empty())
    {
        cout << "Warning: dbtable is not set" << endl;
    }

    if (this->sql.empty())
    {
        cout << "Warning: sql is not set" << endl;
    }
    if (this->dst_table.empty())
    {
        cout << "Warning: dst_table is not set" << endl;
    }
    if (this->spark_master.empty())
    {
        cout << "Warning: spark_master is not set" << endl;
    }
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
        (PyObject* )this->py_class_sparksql,
        PyString_FromString(py_meth_geturl.c_str()),
        PyString_FromString(this->url.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_getuser.c_str()),
        PyString_FromString(this->user.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_getpassword.c_str()),
        PyString_FromString(this->password.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_getsql.c_str()),
        PyString_FromString(this->sql.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_getdsttable.c_str()),
        PyString_FromString(this->dst_table.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
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
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_getdbtable.c_str()),
        pydbtable,
        NULL);

    PyObject_CallMethodObjArgs(
        (PyObject*)this->py_class_sparksql,
        PyString_FromString(py_meth_printall.c_str()),
        NULL);
    Py_DECREF(pysparkmaster);
    Py_DECREF(pydbtable);
    cout << "set values to sparksql.py" << endl;
}
