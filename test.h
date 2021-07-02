//
// Created by josqu on 28/06/2021.
//

#include <iostream>
#include <vector>
#include <sw/redis++/redis++.h>

#ifndef BGPREDISSAVER_TEST_H
#define BGPREDISSAVER_TEST_H

namespace test {
    void test();
    void test_arg_string(std::string str);
    void test_arg_map(std::unordered_map<std::string, double> keys);
}

#endif //BGPREDISSAVER_TEST_H
