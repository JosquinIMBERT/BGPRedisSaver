//
// Created by josqu on 28/06/2021.
//

#include "Ensemble.h"

Ensemble::Ensemble(std::string keys, std::string values, int size) {
    this->key_set_name = keys;
    this->value_set_name = values;
    this->max_size = size;
}

std::string Ensemble::getKeys() {
    return this->key_set_name;
}

std::string Ensemble::getValues() {
    if(!value_set_name.empty())
        return this->value_set_name;
    return "";
}

int Ensemble::getSize() {
    return this->max_size;
}
