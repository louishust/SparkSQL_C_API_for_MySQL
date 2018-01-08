#include "pyspark_exception.h"
void pyspark_exception::pyerr_fetch()
{
    if (PyErr_Occurred())
    {
        PyObject *ptype, *pvalue, *ptraceback;
        PyObject *pystr, *module_name, *pyth_module, *pyth_func;
        char *str;

        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        type = PyString_AsString(PyObject_Str(ptype));
        description = PyString_AsString(PyObject_Str(pvalue));


        /* See if we can get a full traceback */
        module_name = PyString_FromString("traceback");
        pyth_module = PyImport_Import(module_name);
        Py_DECREF(module_name);

        if (pyth_module == NULL)
        {
            return;
        }

        pyth_func = PyObject_GetAttrString(pyth_module, "format_exception");
        if (pyth_func && PyCallable_Check(pyth_func))
        {
            PyObject *pyth_val;

            pyth_val = PyObject_CallFunctionObjArgs(pyth_func, ptype, pvalue, ptraceback, NULL);

            traceback = PyString_AsString(PyObject_Str(pyth_val));
            this->traceback = traceback;
            Py_DECREF(pyth_val);
        }
    }
}