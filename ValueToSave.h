//
// Created by josqu on 29/06/2021.
//

#ifndef BGPREDISSAVER_VALUETOSAVE_H
#define BGPREDISSAVER_VALUETOSAVE_H


#include <string>

class ValueToSave {
public:
    const static int SAVE_UNKNOWN = 0;
    const static int SAVE_ASEvent = 1;
    const static int SAVE_RoutingEvent = 2;
    const static int SAVE_Path = 3;

    ValueToSave(std::string key, std::string val, int type);
    ValueToSave(std::string val, int type);
    std::string getKey();
    std::string getVal();
    int getType();


private:
    std::string key; //Optional
    std::string val;
    int type;
};


#endif //BGPREDISSAVER_VALUETOSAVE_H
