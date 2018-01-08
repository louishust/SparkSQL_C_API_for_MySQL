/*
 * Copyright (C) 2017 Great OpenSource Inc. All Rights Reserved.
 */

#include "sparksql.h"
#include <Python.h>
#include "pyspark_exception.h"
#include <iostream>

using namespace dbscale;

sparksql::sparksql()
{
    cout << endl
         << "init\n"
         << endl;
    Py_Initialize();
    cout << endl
         << "inited\n"
         << endl;
    if (!Py_IsInitialized())
    {
        cout << "Error: Py_IsInitialized(): false" << endl;
        exit(-1);
    }
    PyRun_SimpleString("import sys");
    // path to sparksql.py
    PyRun_SimpleString("sys.path.append('.')");

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
    cout << endl
         << "deleting\n"
         << endl;
    Py_DECREF(this->py_module);
    Py_DECREF(this->py_moduledict);
    Py_DECREF(this->py_class_sparksql);
    cout << endl
         << "deleted\n"
         << endl;
}

void sparksql::pyfinal()
{
    cout << endl
         << "final\n"
         << endl;

    Py_Finalize();
    cout << endl
         << "end\n"
         << endl;
}

void sparksql::run()
{
    this->checkValue();
    cout << "runing" << endl;
    this->setValueToPython();
    string py_meth_run = "run";
    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
        PyString_FromString(py_meth_run.c_str()),
        NULL);

    // PyObject *err = PyErr_Occurred();
    // if (err)
    // {
    //     cout << "###############err occurred##########" << endl;
    // }

    // PyObject *type = NULL, *value = NULL, *traceback = NULL;

    // PyErr_Fetch(&type, &value, &traceback);
    // //PyErr_Restore(type, value, traceback);

    // if (type)
    // {
    //     std::cout << "######################" << endl;
    //     std::cout << PyExceptionClass_Name(type) << ": " << endl;
    // }

    // if (value)
    // {
    //     PyObject *line = PyObject_Str(value);
    //     std::cout << PyString_AS_STRING(line) << endl;
    // }

    // if (traceback)
    // {
    //     PyObject *line = PyObject_Str(traceback);
    //     std::cout << PyString_AS_STRING(line) << endl;
    // }

    // if (PyErr_Occurred())
    // {
    //     PyObject *ptype, *pvalue, *ptraceback;
    //     PyObject *pystr, *module_name, *pyth_module, *pyth_func;
    //     char *str;

    //     PyErr_Fetch(&ptype, &pvalue, &ptraceback);
    //     char *type = PyString_AsString(PyObject_Str(ptype));
    //     char *value = PyString_AsString(PyObject_Str(pvalue));

    //     std::cout << type << endl;
    //     std::cout << value << endl;

    //     /* See if we can get a full traceback */
    //     module_name = PyString_FromString("traceback");
    //     pyth_module = PyImport_Import(module_name);
    //     Py_DECREF(module_name);

    //     if (pyth_module == NULL)
    //     {
    //         return;
    //     }

    //     pyth_func = PyObject_GetAttrString(pyth_module, "format_exception");
    //     if (pyth_func && PyCallable_Check(pyth_func))
    //     {
    //         PyObject *pyth_val;

    //         pyth_val = PyObject_CallFunctionObjArgs(pyth_func, ptype, pvalue, ptraceback, NULL);

    //         char *traceback = PyString_AsString(PyObject_Str(pyth_val));
    //         std::cout << traceback << endl;
    //         Py_DECREF(pyth_val);
    //     }
    // }

    pyspark_exception pe;
    pe.pyerr_fetch();
    cout << pe.type << endl;
    cout << pe.description << endl;
    cout << pe.traceback << endl;

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
        this->py_class_sparksql,
        PyString_FromString(py_meth_geturl.c_str()),
        PyString_FromString(this->url.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
        PyString_FromString(py_meth_getuser.c_str()),
        PyString_FromString(this->user.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
        PyString_FromString(py_meth_getpassword.c_str()),
        PyString_FromString(this->password.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
        PyString_FromString(py_meth_getsql.c_str()),
        PyString_FromString(this->sql.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
        PyString_FromString(py_meth_getdsttable.c_str()),
        PyString_FromString(this->dst_table.c_str()),
        NULL);
    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
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
        this->py_class_sparksql,
        PyString_FromString(py_meth_getdbtable.c_str()),
        pydbtable,
        NULL);

    PyObject_CallMethodObjArgs(
        this->py_class_sparksql,
        PyString_FromString(py_meth_printall.c_str()),
        NULL);
    Py_DECREF(pysparkmaster);
    Py_DECREF(pydbtable);
    cout << "set values to sparksql.py" << endl;
}
