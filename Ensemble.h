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
    std::string getValues(std::string listKey);
    int getSize();
    bool isStatic();
    std::string getDstTable();
    Ensemble(std::string keys, std::string values, int size, bool staticSet, std::string dstTable);
private:
    std::string key_set_name; //Ordered Set (ZSet), contains (timestamp->key)
    std::string value_set_name; //List Prefix (non-static mode, exple : "PRE:" or "ASR:") / Hash (static mode)
    int max_size;
    bool staticSet;
    std::string dstTable;
};


#endif //BGPREDISSAVER_ENSEMBLE_H
