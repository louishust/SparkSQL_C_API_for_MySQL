#ifndef SYD_SPARKSQL_C_API_H_
#define SYD_SPARKSQL_C_API_H_

#include <vector>
#include <string>
using namespace std;

typedef vector<string> VectorString;


void run_sql(
    vector<VectorString> _url_user_password,
    /*
        _url_user_password.size() == 1 || 
        _url_user_password.size == _tables.size()
        {
            string _url,
            string _user,
            string _password
        }
    */
    vector<string> _tables,
    string _sql,
    vector<string> _target,
    /*
        _target.size() == 1 || _target.size() == 4
        {
            string _target_name // necessary
            string _target_rul = "",
            string _target_user = "",
            string _target_password = ""
        }
            
    */
    string _master = "local",
    string _appname = "run_sql",
    vector<string> _tables_alia = vector<string>());

#endif
