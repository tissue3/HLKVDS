#include <iostream>
#include <string>
#include <sstream>
#include "hlkvds/Kvdb.h"
#include "hlkvds/Options.h"

using namespace hlkvds;

void CreateExample(string filename) {

    Options opts;

   opts.hashtable_size = 1280000;
  opts.gc_upper_level = 0.7;
  opts.disable_cache = 1;
    if (!hlkvds::DB::CreateDB(filename, opts)) {
        std::cout << "CreateDB Failed!" << std::endl;
        return;
    }

}

int main(int argc, char** argv) {
    string filename = argv[1];

    CreateExample(filename);
    return 0;
}
