//
// Created by josqu on 24/06/2021.
//

#include <iostream>
#include <vector>
#include <sw/redis++/redis++.h>

#include "Ensemble.h"
#include "BGPCassandraInserter.h"

#ifndef BGPREDISSAVER_BGPREDISSAVER_H
#define BGPREDISSAVER_BGPREDISSAVER_H

class BGPRedisSaver {
public:
//#################### METHODES PUBLIQUES ####################
    void init_connections();
    void end_connections();
    void run(std::vector<Ensemble> sets);
    void stopTransfer();
    void setRedis(std::string redis_host, int redis_port);
    void setBatchMaxSize(int max_size);
    void setCassandra(std::string cassandra_host, int cassandra_port);
    void setPrint(bool p);
    void setSleepDuration(int sleep);

private:
//#################### ATTRIBUTS PRIVES ####################
    bool stop = false;
    bool print = false;
    int sleep_duration = 2;
    int BATCH_MAX_SIZE = 1000;

    std::string redis_host="127.0.0.1";
    int redis_port=6379;
    sw::redis::Redis redis = sw::redis::Redis("tcp://127.0.0.1:6379");

    BGPCassandraInserter cass_inserter;


//#################### METHODES PRIVEES ####################
    void getKeysToDelete(std::string keys_set_name, std::string type, int nb_to_del, std::unordered_map<std::string, double> *data);
    void getOldValues(std::string values_set_name, std::string type, std::string key, std::vector<std::string> *toSave);
    void deleteKeys(std::string keys_set_name, std::string type, int start, int stop);
    void deleteValues(std::string values_set_name, std::string type, std::string old_key, bool isStatic);
    int getStructSize(std::string keys_set_name, std::string type);

    void printSetInfo(std::string keys_set_name, std::string type, int set_size, int nb_element, int nb_to_del);
    void printInfo(std::string initial_tab, int ind, std::string values_set_name, std::string old_key, std::vector<std::string> toSave, int success);

};

#endif //BGPREDISSAVER_BGPREDISSAVER_H
