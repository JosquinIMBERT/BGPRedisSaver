//
// Created by josqu on 28/06/2021.
//
#include <unistd.h>
#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include "bgpstream.h"

#include "test.h"

using namespace sw::redis;
using namespace std;

sw::redis::QueuedRedis<sw::redis::PipelineImpl> *pipe_redis;

void test::test() {
    cout << "vvvvvvvvvvvvvvvvvvvvvvvv Tests vvvvvvvvvvvvvvvvvvvvvvvv" << endl;
    ConnectionOptions connection_options;
    connection_options.host = "127.0.0.1";
    connection_options.port = 6379;
    auto redis = Redis(connection_options);
    auto pipeline = redis.pipeline();
    pipe_redis = &pipeline;
    pipe_redis->zadd("MonZSet", "Une valeur", 354343843);
    pipe_redis->zremrangebyrank("MonZSet", 0, 0);
    pipe_redis->lpush("MaListe","value");
    pipe_redis->lpush("MaListe","value");
    pipe_redis->exec();
    pipe_redis->exec();
    cout << "^^^^^^^^^^^^^^^^^^^^^^^^ Tests ^^^^^^^^^^^^^^^^^^^^^^^^" << endl;
}