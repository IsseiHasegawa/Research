#include "Server.h"
#include <iostream>

int main() {
    Server s(11211);
    std::string err;
    if (!s.start(err)) {
        std::cerr << "start failed: " << err << "\n";
        return 1;
    }
    s.run();
    return 0;
}