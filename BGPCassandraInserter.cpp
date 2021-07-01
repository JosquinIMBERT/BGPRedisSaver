//
// Created by josqu on 25/06/2021.
//

#include <iostream>
#include <cassandra.h>

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

    void insert(string dstTable, string set_name, string old_key, string old_value, int old_timestamp) {
        string table_insertion = "";
        if(set_name=="APATHS") {
            table_insertion = "Paths";
        } else if(set_name=="ROUTINGENTRIES") {

        } else { //Default : insert key - value

        }

        const char *query = ("INSERT INTO BGP_KEYSPACE."+table_insertion+" (key, value, timestamp) VALUES (?,?,?)").c_str();
        cout << query << endl;
        /*CassStatement *statement = cass_statement_new(query, 3);
        cass_statement_bind_string(statement, 0, old_key);
        cass_statement_bind_string(statement, 1, old_value);
        cass_statement_bind_int32(statement, 2, old_timestamp);
        CassFuture *result_future = cass_session_execute(session, statement);
        if (cass_future_error_code(result_future) == CASS_OK) {
            cout << "La donnée (key:" << old_key
                 <<",value:" << old_value
                 <<",timestamp:" << old_timestamp
                 <<") a bien été transférée vers Cassandra." << endl;
        } else {
            const char *message;
            size_t message_length;
            cass_future_error_message(result_future, &message, &message_length);
            fprintf(stderr, "Unable to run query: '%.*s'\n", (int) message_length, message);
        }*/
    }

    void setCassandra(string cassandra_host, int cassandra_port) {
        BGPCassandraInserter::cassandra_host = cassandra_host;
        BGPCassandraInserter::cassandra_port = cassandra_port;
    }






    void addAS() {

    }

    void addPath() {

    }

    void addLink() {

    }

}