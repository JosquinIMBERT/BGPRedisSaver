//
// Created by josqu on 29/06/2021.
//

#include "ValueToSave.h"

using namespace std;

ValueToSave::ValueToSave(string key, string val, int type) {
    this->key = key;
    this->val = val;
    this->type = type;
}

ValueToSave::ValueToSave(string val, int type) {
    this->key = "";
    this->val = val;
    this->type = type;
}

string ValueToSave::getKey() {
    return this->key;
}

string ValueToSave::getVal() {
    return this->val;
}

int ValueToSave::getType() {
    return this->type;
}