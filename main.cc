#include "sparksql_c_api.h"
#include <iostream>
using namespace std;
using namespace sparksql;
int main()
{
    vector<VectorString> _url_user_password_vector_vector;
    VectorString _tables_vector;
    string _sql_string;
    string _target_name_string;
    VectorString _target_vector = VectorString();
    string _master_string = "local";
    string _appname_string = "run_sql";
    VectorString _tables_alias_vector = VectorString();
    run_sql(_url_user_password_vector_vector,
            _tables_vector,
            _sql_string,
            _target_name_string,
            _target_vector,
            _master_string,
            _appname_string,
            _tables_alias_vector);
    cout << "success" << endl;
    return 0;
}
