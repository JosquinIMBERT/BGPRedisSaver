//
// Created by josqu on 28/06/2021.
//
#include <unistd.h>
#include <iostream>
#include <thread>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include <cassandra.h>
#include "bgpstream.h"

#include "test.h"

using namespace sw::redis;
using namespace std;

sw::redis::QueuedRedis<sw::redis::PipelineImpl> *pipe_redis;

void MyPersonnelThread(mutex *m) {
    mutex *mtx = m;
    cout << "MyThread - mtx: " << mtx->try_lock() << endl;
    mtx->lock();
    cout << "MyThread - locked" << endl;
    cout << "MyThread - mtx: " << mtx->try_lock() << endl;
    mtx->unlock();
    cout << "MyThread - unlocked" << endl;
}

void test::test() {
    cout << "vvvvvvvvvvvvvvvvvvvvvvvv Tests vvvvvvvvvvvvvvvvvvvvvvvv" << endl;

    mutex mtx;
    mtx.lock();
    cout << "MainThread - locked" << endl;
    thread t(MyPersonnelThread, &mtx);
    sleep(5);
    mtx.unlock();
    cout << "MainThread - unlocked" << endl;
    t.join();

    cout << "^^^^^^^^^^^^^^^^^^^^^^^^ Tests ^^^^^^^^^^^^^^^^^^^^^^^^" << endl;
}