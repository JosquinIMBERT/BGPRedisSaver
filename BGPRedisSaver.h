//
// Created by josqu on 24/06/2021.
//

#include <iostream>
#include <vector>

#include "Ensemble.h"
#include "ValueToSave.h"

#ifndef BGPREDISSAVER_BGPREDISSAVER_H
#define BGPREDISSAVER_BGPREDISSAVER_H

namespace BGPRedisSaver {
    void init_connections();
    void end_connections();
    void run(std::vector<Ensemble> sets);
    void stopTransfer();
    void setRedis(std::string redis_host, int redis_port);
    void setCassandra(std::string cassandra_host, int cassandra_port);
    const char *getOldValue(std::string set, std::string key);
    void getOldValues(std::string values_set_name, std::string key, std::vector<ValueToSave> toSave);
    int getStructSize(std::string keys_set_name);
}

#endif //BGPREDISSAVER_BGPREDISSAVER_H
