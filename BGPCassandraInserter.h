//
// Created by josqu on 25/06/2021.
//

#include <iostream>

#ifndef BGPREDISSAVER_BGPCASSANDRAINSERTER_H
#define BGPREDISSAVER_BGPCASSANDRAINSERTER_H

namespace BGPCassandraInserter {

    void init_connections();
    void end_connections();
    void insert(std::string set_name, std::string old_key, std::string old_value, int old_timestamp);
    void setCassandra(std::string cassandra_host, int cassandra_port);
}

#endif //BGPREDISSAVER_BGPCASSANDRAINSERTER_H
