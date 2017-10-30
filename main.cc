#include "sparksql.h"
#include <iostream>
using namespace std;
using namespace dbscale;
int main()
{
    sparksql ss;
    ss.run();
    ss.url = "something";
    cout << ss.url << endl;
    return 0;
}
