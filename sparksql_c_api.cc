#include "sparksql_c_api.h"
#include <iostream>
#include <Python.h>
using namespace std;

void sparksql::run_sql(
    vector<VectorString> _url_user_password_vector_vector,
    VectorString _tables_vector,
    string _sql_string,
    string _target_name_string,
    VectorString _target_vector,
    string _master_string,
    string _appname_string,
    VectorString _tables_alias_vector)
{
    //import moudle and get function
    char program_name[] = "run_sql_on_spark";
    Py_SetProgramName(program_name);
    Py_Initialize();
    PyObject *pMoudle;
    pMoudle = PyImport_Import(PyString_FromString("sparksql_c_api"));
    if (pMoudle == NULL)
    {
        cout << "Failed to import moudle <sparksql_c_api>." << endl;
        return;
    }
    PyObject *pFuc_runsql;
    pFuc_runsql = PyObject_GetAttrString(pMoudle, "run_sql");

    // translate c++ args to python args
    PyObject *pArgs;
    // vector<VectorString> _url_user_password_vector_vector
    PyObject *pTupleTuple_url_user_password;
    for (int i = 0; i < _url_user_password_vector_vector.size(); i++)
    {
        PyObject *pTuple_uup;
        PyTuple_SetItem(
            pTuple_uup, 0,
            PyString_FromString(_url_user_password_vector_vector[i][0].c_str()));
        PyTuple_SetItem(
            pTuple_uup, 1,
            PyString_FromString(_url_user_password_vector_vector[i][1].c_str()));
        PyTuple_SetItem(
            pTuple_uup, 2,
            PyString_FromString(_url_user_password_vector_vector[i][2].c_str()));
        PyTuple_SetItem(pTupleTuple_url_user_password, i, pTuple_uup);
    }
    // VectorString _tables_vector
    PyObject *pTupele_tables;
    for (int i = 0; i < _tables_vector.size(); i++)
        PyTuple_SetItem(
            pTupele_tables, i, PyString_FromString(_tables_vector[i].c_str()));
    // string _sql_string
    PyObject *pString_sql = PyString_FromString(_sql_string.c_str());
    // string _target_name_string
    PyObject *pString_target_name = PyString_FromString(_target_name_string.c_str());
    // VectorString _target_vector (url,user,password)
    PyObject *pTuple_target;
    PyTuple_SetItem(
        pTuple_target, 0, PyString_FromString(_tables_vector[0].c_str()));
    PyTuple_SetItem(
        pTuple_target, 1, PyString_FromString(_tables_vector[1].c_str()));
    PyTuple_SetItem(
        pTuple_target, 2, PyString_FromString(_tables_vector[2].c_str()));
    // string _master_string
    PyObject *pString_master = PyString_FromString(_master_string.c_str());
    // string _appname_string
    PyObject *pString_appname = PyString_FromString(_appname_string.c_str());
    // VectorString _tables_alias_vector
    PyObject *pTupele_tables_alias;
    for (int i = 0; i < _tables_alias_vector.size(); i++)
        PyTuple_SetItem(
            pTupele_tables_alias, i,
            PyString_FromString(_tables_alias_vector[i].c_str()));

    // vector<VectorString> _url_user_password_vector_vector,
    // VectorString _tables_vector,
    // string _sql_string,
    // string _target_name_string,
    // VectorString _target_vector,
    // string _master_string,
    // string _appname_string,
    // VectorString _tables_alias_vector
    PyTuple_SetItem(pArgs, 0, pTupleTuple_url_user_password);
    PyTuple_SetItem(pArgs, 1, pTupele_tables);
    PyTuple_SetItem(pArgs, 2, pString_sql);
    PyTuple_SetItem(pArgs, 3, pString_target_name);
    PyTuple_SetItem(pArgs, 4, pTuple_target);
    PyTuple_SetItem(pArgs, 5, pString_master);
    PyTuple_SetItem(pArgs, 6, pString_appname);
    PyTuple_SetItem(pArgs, 7, pTupele_tables_alias);
    PyObject_CallObject(pFuc_runsql, pArgs);
    Py_Finalize();
}
