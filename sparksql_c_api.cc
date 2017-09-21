#include "sparksql_c_api.h"

void sparksql::run_sql(
    vector<VectorString> _url_user_password_vector_vector,
    VectorString _tables_vector,
    string _sql_string,
    string _target_name_string,
    VectorString _target_vector,
    string _master_string ,
    string _appname_string ,
    VectorString _tables_alias_vector)
{
    _url_user_password_vector_vector = vector<VectorString>();
    _tables_vector = VectorString();
    _sql_string = "";
    _target_name_string = "";
    _target_vector = VectorString();
    _master_string = "local";
    _appname_string = "run_sql";
    _tables_alias_vector = VectorString();
}
