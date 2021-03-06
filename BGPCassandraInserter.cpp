//
// Created by josqu on 25/06/2021.
//

#include <iostream>
#include <cassandra.h>
#include <vector>
#include <time.h>
#include <boost/algorithm/string.hpp>

#include "BGPCassandraInserter.h"
#include "RedisDecode.h"

using namespace std;


BGPCassandraInserter::BGPCassandraInserter() {}
BGPCassandraInserter::BGPCassandraInserter(std::string host, int port, int batchMaxSize) {
    this->cassandra_host = host;
    this->cassandra_port = port;
    this->BATCH_MAX_SIZE = 100;//batchMaxSize;
}


//######################## CONNEXION ########################
void BGPCassandraInserter::init_connections() {
    //##### Connexion à la base cassandra #####
    CassFuture *connect_future = NULL;
    cluster = cass_cluster_new();
    session = cass_session_new();
    batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);
    batch_size = 0;
    // Add contact points
    cass_cluster_set_contact_points(cluster, cassandra_host.c_str());
    cass_cluster_set_port(cluster, cassandra_port);
    // Provide the cluster object as configuration to connect the session
    connect_future = cass_session_connect_keyspace(session, cluster, KEYSPACE);
    if (cass_future_error_code(connect_future) != CASS_OK) {
        cout << "Connection to Cassandra: failed.\n";
        cass_future_free(connect_future);
        cass_cluster_free(cluster);
        cass_session_free(session);
        exit(1);
    }
    cass_future_free(connect_future);
    cout << "Connection to Cassandra: done.\n";
}

void BGPCassandraInserter::end_connections() {
    //##### Exécution des dernières commandes #####
    cass_session_execute_batch(session, batch);

    //##### Fin de la connexion à la base Cassandra #####
    CassFuture *close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    //##### Libération des variables Cassandra #####
    cass_batch_free(batch);
    cass_cluster_free(cluster);
    cass_session_free(session);
    cout << "Ending connection with Cassandra: done.\n";
}







//######################## FONCTION PRINCIPALE ########################
int BGPCassandraInserter::insert(string dstTable, string set_name, string old_key, string old_value, unsigned int old_timestamp) {
    CassStatement *statement;
    if(boost::iequals(dstTable, "ROUTINGEVENT")) {
        statement = addRoutingEventQuery(old_key, old_value);
    } else if(boost::iequals(dstTable, "ASEVENT")) {
        statement = addASEventQuery(old_key, old_value);
    } else if(boost::iequals(dstTable, "PATH")) {
        statement = addPathQuery(old_value);
    } else {
        statement = addDefaultQuery(dstTable, set_name, old_key, old_value, old_timestamp);
    }


    cass_batch_add_statement(batch, statement);
    if(++batch_size >= BATCH_MAX_SIZE) {
        CassFuture *result_future = cass_session_execute_batch(session, batch);

        cass_batch_free(batch);
        batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);

        int ret = batch_size;
        batch_size = 0;
        if (cass_future_error_code(result_future) != CASS_OK) {
            const char *message;
            size_t message_length;
            cass_future_error_message(result_future, &message, &message_length);
            fprintf(stderr, "BGPCassandraInserter error: Unable to run query: '%.*s'\n", (int) message_length, message);
            return 0;
        }
        return ret;
    }
    return 0;
}







//######################## DONNEES DE CONNEXION ########################
void BGPCassandraInserter::setCassandra(string cassandra_host, int cassandra_port) {
    BGPCassandraInserter::cassandra_host = cassandra_host;
    BGPCassandraInserter::cassandra_port = cassandra_port;
}

void BGPCassandraInserter::setBatchMaxSize(int max_size) {
    BATCH_MAX_SIZE = max_size;
}






//######################## ADAPTATION A LA DONNEE A INSERER ########################

CassStatement *BGPCassandraInserter::addRoutingEventQuery(string old_key, string old_value) {
    const char *query = "INSERT INTO ROUTINGEVENT (prefixID, prefix, peer, pathHash, active, time, status) VALUES (?, ?, ?, ?, ?, ?, ?)";

    string prefixID = "", prefix = "", pathHash = "", status = "", active = "";
    int peer = 0;
    long unsigned int time = 0;

    //Initialisation et décodage
    RedisDecode::routingEventFromRedis(old_value, &pathHash, &status, &active, &time);
    RedisDecode::routingEventFromRedisKey(old_key, &prefixID, &prefix, &peer);

    CassStatement *statement = cass_statement_new(query, 7);
    cass_statement_bind_string(statement, 0, prefixID.c_str());
    cass_statement_bind_string(statement, 1, prefix.c_str());
    cass_statement_bind_int32(statement, 2, peer);
    cass_statement_bind_string(statement, 3, pathHash.c_str());
    cass_statement_bind_string(statement, 4, active.c_str());
    cass_statement_bind_int64(statement, 5, time*1000);
    cass_statement_bind_string(statement, 6, status.c_str());

    return statement;
}

CassStatement *BGPCassandraInserter::addASEventQuery(string old_key, string old_value) {
    const char *query = "INSERT INTO ASEVENT (dstAS, ASN, prefixID, active, time) VALUES (?, ?, ?, ?, ?)";

    string dstAS = "";
    int ASN = 0;
    string prefixID = "", active = "";
    long unsigned int time = 0;

    RedisDecode::ASEventFromRedis(old_value, &prefixID, &active, &time);
    RedisDecode::ASEventFromRedisKey(old_key, &dstAS, &ASN);

    CassStatement *statement = cass_statement_new(query, 5);
    cass_statement_bind_string(statement, 0, dstAS.c_str());
    cass_statement_bind_int32(statement, 1, ASN);
    cass_statement_bind_string(statement, 2, prefixID.c_str());
    cass_statement_bind_string(statement, 3, active.c_str());
    cass_statement_bind_int64(statement, 4, time*1000);

    return statement;
}

CassStatement *BGPCassandraInserter::addPathQuery(string old_value) {
    const char *query = "INSERT INTO PATH (hash, path, pathLength, prefNum, lastChange, meanUp, meanDown, collector, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    string hash = "", collector = "";
    vector<string> path;
    int pathLength = 0, prefNum = 0;
    long unsigned int lastChange = 0;
    double meanUp = 0.0, meanDown = 0.0;
    bool active = true;

    RedisDecode::pathFromRedis(old_value, &hash, &collector, &path, &pathLength, &prefNum, &lastChange, &meanUp,
                               &meanDown, &active);

    CassStatement *statement = cass_statement_new(query, 9);
    cass_statement_bind_string(statement, 0, hash.c_str());
    CassCollection_ *collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, path.size());
    for(auto node : path) {cass_collection_append_string(collection, node.c_str());}
    cass_statement_bind_collection(statement, 1, collection);
    cass_statement_bind_int32(statement, 2, pathLength);
    cass_statement_bind_int32(statement, 3, prefNum);
    cass_statement_bind_int64(statement, 4, lastChange*1000);
    cass_statement_bind_double(statement, 5, meanUp);
    cass_statement_bind_double(statement, 6, meanDown);
    cass_statement_bind_string(statement, 7, collector.c_str());
    cass_statement_bind_bool(statement, 8, static_cast<cass_bool_t>(active));

    return statement;
}

CassStatement *BGPCassandraInserter::addDefaultQuery(string dstTable, string set_name, string old_key, string old_value, unsigned int old_timestamp) {
    const char *query = "INSERT INTO DEFAULT_TABLE (key, value, dstTable, srcSet, time) VALUES (?, ?, ?, ?, ?)";

    CassStatement *statement = cass_statement_new(query, 5);
    cass_statement_bind_string(statement, 0, old_key.c_str());
    cass_statement_bind_string(statement, 1, old_value.c_str());
    cass_statement_bind_string(statement, 2, dstTable.c_str());
    cass_statement_bind_string(statement, 3, set_name.c_str());
    cass_statement_bind_int64(statement, 4, old_timestamp*1000);
    return statement;
}