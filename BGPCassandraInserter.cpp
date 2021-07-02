//
// Created by josqu on 25/06/2021.
//

#include <iostream>
#include <cassandra.h>
#include <vector>
#include <time.h>

#include "BGPCassandraInserter.h"

using namespace std;

namespace BGPCassandraInserter {
    CassCluster *cluster;
    CassSession *session;
    string cassandra_host="127.0.0.1";
    int cassandra_port=9042;

    void init_connections() {
        cout << "TODO init cassandra connection" << endl;
    }

    void end_connections() {
        cout << "TODO end cassandra connection" << endl;
    }

    void insert(string dstTable, string set_name, string old_key, string old_value, unsigned int old_timestamp) {
        CassStatement *statement;
        if(dstTable=="ROUTINGEVENT") {
            statement = addRoutingEventQuery(old_value);
        } else if(dstTable=="ASEVENT") {
            statement = addASEventQuery(old_value);
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

    void setCassandra(string cassandra_host, int cassandra_port) {
        BGPCassandraInserter::cassandra_host = cassandra_host;
        BGPCassandraInserter::cassandra_port = cassandra_port;
    }



    CassStatement *addRoutingEventQuery(string old_value) {
        char *query = "INSERT INTO ROUTINGEVENT"
                      " (prefixID, peer, pathHash, active, time, status)"
                      " VALUES (?, ?, ?, ?, ?, ?)";

        string prefixID = "", pathHash = "", status = "", active = "";
        int peer = 0;
        unsigned int time = 0;

        //TODO initialiser les valeurs en decodant

        CassStatement *statement = cass_statement_new(query, 6);
        cass_statement_bind_string(statement, 0, prefixID.c_str());
        cass_statement_bind_int32(statement, 1, peer);
        cass_statement_bind_string(statement, 2, pathHash.c_str());
        cass_statement_bind_string(statement, 3, active.c_str());
        cass_statement_bind_uint32(statement, 4, time);
        cass_statement_bind_string(statement, 5, status.c_str());

        return statement;
    }

    CassStatement *addASEventQuery(string old_value) {
        char *query = "INSERT INTO ASEVENT"
                      " (dstAS, prefixID, active, time)"
                      " VALUES (?, ?, ?, ?)";

        int dstAS = 0;
        string prefixID = "", active = "";
        unsigned int time = 0;

        //TODO initialiser les valeurs en décodant

        CassStatement *statement = cass_statement_new(query, 6);
        cass_statement_bind_int32(statement, 0, dstAS);
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

        //TODO initialiser les valeurs en décodant

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