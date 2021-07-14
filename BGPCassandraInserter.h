//
// Created by josqu on 25/06/2021.
//

#include <iostream>
#include <cassandra.h>

#ifndef BGPREDISSAVER_BGPCASSANDRAINSERTER_H
#define BGPREDISSAVER_BGPCASSANDRAINSERTER_H

class BGPCassandraInserter {
public:
//#################### METHODES PUBLIQUES ####################
    void init_connections();
    void end_connections();
    int insert(std::string dstTable, std::string set_name, std::string old_key, std::string old_value, unsigned int old_timestamp);
    void setCassandra(std::string cassandra_host, int cassandra_port);
    void setBatchMaxSize(int max_size);

private:
//#################### ATTRIBUTS PRIVES ####################
    CassCluster *cluster;
    CassSession *session;

    CassBatch *batch;
    int batch_size;
    int BATCH_MAX_SIZE = 1000;

    std::string cassandra_host="127.0.0.1";
    int cassandra_port=9042;
    const char *KEYSPACE="BGP_KEYSPACE";


//#################### METHODES PRIVEES ####################
    CassStatement *addRoutingEventQuery(std::string old_key, std::string old_value);
    CassStatement *addASEventQuery(std::string old_key, std::string old_value);
    CassStatement *addPathQuery(std::string old_value);
    CassStatement *addDefaultQuery(std::string dstTable, std::string set_name, std::string old_key, std::string old_value, unsigned int old_timestamp);

};

#endif //BGPREDISSAVER_BGPCASSANDRAINSERTER_H
