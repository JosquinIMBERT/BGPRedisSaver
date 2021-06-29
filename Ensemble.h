//
// Created by josqu on 28/06/2021.
//

#ifndef BGPREDISSAVER_ENSEMBLE_H
#define BGPREDISSAVER_ENSEMBLE_H

#include <string>

class Ensemble {
public:
    std::string getKeys();
    std::string getValues();
    int getSize();
    std::string getDstTable();
    Ensemble(std::string keys, std::string values, int size);
private:
    std::string key_set_name; //Ordered Set (ZSet), contains (timestamp->key)
    std::string value_set_name; //Unordered Set (Set), contains (key->value)
    int max_size;
    std::string dstTable;
};


#endif //BGPREDISSAVER_ENSEMBLE_H
