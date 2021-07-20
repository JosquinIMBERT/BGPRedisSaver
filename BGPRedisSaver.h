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
    static const std::string CHANNEL_END;
    static bool stop;
    static std::mutex *mtx_print;
//#################### METHODES PUBLIQUES ####################
    BGPRedisSaver(const BGPRedisSaver &src);
    BGPRedisSaver(int i,
                  //std::mutex *mtx_stop,
                  bool print,
                  int sleep_duration,
                  int BATCH_MAX_SIZE,
                  std::string redis_host,
                  int redis_port,
                  std::string cassandra_host,
                  int cassandra_port,
                  std::vector<Ensemble> *sets);
    void init_connections();
    void end_connections();
    void run();


private:
//#################### ATTRIBUTS PRIVES ####################
    int id;
    bool print = false;
    int sleep_duration = 0;
    int BATCH_MAX_SIZE = 1000;

    std::string redis_host="127.0.0.1";
    int redis_port=6379;
    sw::redis::Redis redis = sw::redis::Redis("tcp://127.0.0.1:6379");
    sw::redis::QueuedRedis<sw::redis::PipelineImpl> redis_pipe = redis.pipeline();

    std::vector<Ensemble> *sets;
    BGPCassandraInserter cass_inserter;


//#################### METHODES PRIVEES ####################
    void getRemoveKeys(std::string keys_set_name,
                      std::string type,
                      int nb_to_del,
                      std::unordered_map<std::string, double> *keys);
    void getRemoveValues(std::string values_set_name,
                         std::string values_type,
                         std::string old_key,
                         std::vector<std::string> *toSave);
    void getValues(std::string values_set_name, std::string type, std::string key, std::vector<std::string> *toSave);
    void removeValues(std::string values_set_name, std::string type, std::string old_key);
    int getStructSize(std::string keys_set_name, std::string type);

    void printSetInfo(std::string keys_set_name, std::string type, int set_size, int nb_element, int nb_to_del);
    void printInfo(std::string initial_tab, int ind, std::string values_set_name, std::string old_key, std::vector<std::string> toSave, int success);

};

#endif //BGPREDISSAVER_BGPREDISSAVER_H
