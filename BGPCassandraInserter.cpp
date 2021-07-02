//
// Created by josqu on 25/06/2021.
//

#include <iostream>
#include <cassandra.h>
#include <vector>
#include <time.h>

#include "BGPCassandraInserter.h"
#include "RedisDecode.h"

using namespace std;

namespace BGPCassandraInserter {
    CassCluster *cluster;
    CassSession *session;
    string cassandra_host="127.0.0.1";
    int cassandra_port=9042;
    const char *KEYSPACE="BGP_KEYSPACE";






    //######################## CONNEXION ########################
    void init_connections() {
        //##### Connexion à la base cassandra #####
        CassFuture *connect_future = NULL;
        cluster = cass_cluster_new();
        session = cass_session_new();
        // Add contact points
        cass_cluster_set_contact_points(cluster, cassandra_host.c_str());
        cass_cluster_set_port(cluster, cassandra_port);
        // Provide the cluster object as configuration to connect the session
        connect_future = cass_session_connect_keyspace(session, cluster, KEYSPACE);
        if (cass_future_error_code(connect_future) != CASS_OK) {
            cerr << "Impossible de se connecter à la base Cassandra. Terminaison du programme." << endl;
            cass_future_free(connect_future);
            cass_cluster_free(cluster);
            cass_session_free(session);
            exit(1);
        }
        cass_future_free(connect_future);
    }

    void end_connections() {
        //##### Fin de la connexion à la base Cassandra #####
        CassFuture *close_future = cass_session_close(session);
        cass_future_wait(close_future);
        cass_future_free(close_future);

        //##### Libération des variables Cassandra #####
        cass_cluster_free(cluster);
        cass_session_free(session);
    }







    //######################## FONCTION PRINCIPALE ########################
    void insert(string dstTable, string set_name, string old_key, string old_value, unsigned int old_timestamp) {
        CassStatement *statement;
        if(dstTable=="ROUTINGEVENT") {
            statement = addRoutingEventQuery(old_key, old_value);
        } else if(dstTable=="ASEVENT") {
            statement = addASEventQuery(old_key, old_value);
        } else if(dstTable=="PATHS") {
            statement = addPathQuery(old_value);
        } else {
            statement = addDefaultQuery(dstTable, set_name, old_key, old_value, old_timestamp);
        }

        CassFuture *result_future = cass_session_execute(session, statement);
        if (cass_future_error_code(result_future) == CASS_OK) {
            cout << "La donnée (key:" << old_key
                 <<", value:" << old_value
                 <<", timestamp:" << old_timestamp
                 <<") a bien été transférée vers Cassandra." << endl;
        } else {
            const char *message;
            size_t message_length;
            cass_future_error_message(result_future, &message, &message_length);
            fprintf(stderr, "Unable to run query: '%.*s'\n", (int) message_length, message);
        }
    }







    //######################## DONNEES DE CONNEXION ########################
    void setCassandra(string cassandra_host, int cassandra_port) {
        BGPCassandraInserter::cassandra_host = cassandra_host;
        BGPCassandraInserter::cassandra_port = cassandra_port;
    }








    //######################## ADAPTATION A LA DONNEE A INSERER ########################

    CassStatement *addRoutingEventQuery(string old_key, string old_value) {
        char *query = "INSERT INTO ROUTINGEVENT"
                      " (prefixID, prefix, peer, pathHash, active, time, status)"
                      " VALUES (?, ?, ?, ?, ?, ?, ?)";

        string prefixID = "", prefix = "", pathHash = "", status = "", active = "";
        int peer = 0;
        unsigned int time = 0;

        //Initialisation et décodage
        RedisDecode::routingEventFromRedis(old_value, &pathHash, &status, &active, &time);
        RedisDecode::routingEventFromRedisKey(old_key, &prefixID, &prefix, &peer);

        CassStatement *statement = cass_statement_new(query, 7);
        cass_statement_bind_string(statement, 0, prefixID.c_str());
        cass_statement_bind_string(statement, 1, prefix.c_str());
        cass_statement_bind_int32(statement, 2, peer);
        cass_statement_bind_string(statement, 3, pathHash.c_str());
        cass_statement_bind_string(statement, 4, active.c_str());
        cass_statement_bind_uint32(statement, 5, time);
        cass_statement_bind_string(statement, 6, status.c_str());

        return statement;
    }

    CassStatement *addASEventQuery(string old_key, string old_value) {
        char *query = "INSERT INTO ASEVENT"
                      " (dstAS, ASN, prefixID, active, time)"
                      " VALUES (?, ?, ?, ?, ?)";

        string dstAS = "";
        int ASN = 0;
        string prefixID = "", active = "";
        unsigned int time = 0;

        RedisDecode::ASEventFromRedis(old_value, &prefixID, &active, &time);
        RedisDecode::ASEventFromRedisKey(old_key, &dstAS, &ASN);

        CassStatement *statement = cass_statement_new(query, 7);
        cass_statement_bind_string(statement, 0, dstAS.c_str());
        cass_statement_bind_int32(statement, 1, ASN);
        cass_statement_bind_string(statement, 1, prefixID.c_str());
        cass_statement_bind_string(statement, 2, active.c_str());
        cass_statement_bind_uint32(statement, 3, time);

        return statement;
    }

    CassStatement *addPathQuery(string old_value) {
        char *query = "INSERT INTO PATH"
                      " (hash, path, pathLength, prefNum, lastChange, meanUp, meanDown, collector, active)"
                      " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        string hash = "", collector = "";
        vector<string> path;
        int pathLength = 0, prefNum = 0;
        unsigned int lastChange = 0;
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
        cass_statement_bind_uint32(statement, 4, lastChange);
        cass_statement_bind_double(statement, 5, meanUp);
        cass_statement_bind_double(statement, 6, meanDown);
        cass_statement_bind_string(statement, 7, collector.c_str());
        cass_statement_bind_bool(statement, 8, static_cast<cass_bool_t>(active));

        return statement;
    }

    CassStatement *addDefaultQuery(string dstTable, string set_name, string old_key, string old_value, unsigned int old_timestamp) {
        char *query = "INSERT INTO DEFAULT_TABLE"
                      " (key, value, dstTable, srcSet, time)"
                      " VALUES (?, ?, ?, ?, ?)";

        CassStatement *statement = cass_statement_new(query, 5);
        cass_statement_bind_string(statement, 0, old_key.c_str());
        cass_statement_bind_string(statement, 1, old_value.c_str());
        cass_statement_bind_string(statement, 2, dstTable.c_str());
        cass_statement_bind_string(statement, 3, set_name.c_str());
        cass_statement_bind_uint32(statement, 4, old_timestamp);
        return statement;
    }

}