//
// Created by josqu on 28/06/2021.
//
#include <iostream>

#include "Ensemble.h"

using namespace std;

Ensemble::Ensemble(std::string keys, std::string values, int size, bool staticSet, string dstTable) {
    this->key_set_name = keys;
    this->value_set_name = values;
    this->max_size = size;
    this->staticSet = staticSet;
    this->dstTable = dstTable;
}

std::string Ensemble::getKeys() {
    return this->key_set_name;
}

std::string Ensemble::getValues() {
    if(!this->staticSet) return "";
    return this->value_set_name;
}

std::string Ensemble::getValues(std::string listKey) {
    if(this->staticSet) return "";
    return this->value_set_name + ":" + listKey;
}

int Ensemble::getSize() {
    return this->max_size;
}

bool Ensemble::isStatic() {
    return this->staticSet;
}

std::string Ensemble::getDstTable() {
    return this->dstTable;
}

std::string Ensemble::toString() {
    string str = "{";
    str += this->key_set_name + ",";
    str += this->value_set_name + ",";
    str += to_string(this->max_size) + ",";
    str += to_string(this->staticSet) + ",";
    str += this->dstTable;
    str += "}";
    return str;
}