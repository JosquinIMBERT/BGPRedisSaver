//
// Created by josqu on 25/06/2021.
//

#include <iostream>
#include <cassandra.h>

#ifndef BGPREDISSAVER_BGPCASSANDRAINSERTER_H
#define BGPREDISSAVER_BGPCASSANDRAINSERTER_H

namespace BGPCassandraInserter {

    void init_connections();
    void end_connections();
    void insert(std::string dstTable, std::string set_name, std::string old_key, std::string old_value, unsigned int old_timestamp);
    void setCassandra(std::string cassandra_host, int cassandra_port);

    CassStatement *addRoutingEventQuery(std::string old_value);
    CassStatement *addASEventQuery(std::string old_value);
    CassStatement *addPathQuery(std::string old_value);
    CassStatement *addDefaultQuery(std::string dstTable, std::string set_name, std::string old_key, std::string old_value, unsigned int old_timestamp);
}

#endif //BGPREDISSAVER_BGPCASSANDRAINSERTER_H
