//
// Created by josqu on 24/06/2021.
//

#include <iostream>
#include <vector>
#include <sw/redis++/redis++.h>

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
    void getKeysToDelete(std::string keys_set_name, int nb_to_del, std::unordered_map<std::string, double> *data);
    void getOldValues(std::string values_set_name, std::string key, std::vector<std::string> toSave);
    void deleteKeys(std::string keys_set_name, int start, int stop);
    void deleteValues(std::string values_set_name, std::string old_key, bool isStatic);
    int getStructSize(std::string keys_set_name);
}

#endif //BGPREDISSAVER_BGPREDISSAVER_H
