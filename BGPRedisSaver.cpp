//
// Created by josqu on 24/06/2021.
//

#include <iostream>
#include <unistd.h>
#include <vector>
#include <cassandra.h>
#include <sw/redis++/redis++.h>

#include "BGPRedisSaver.h"
#include "BGPCassandraInserter.h"
#include "Ensemble.h"
#include "ValueToSave.h"

using namespace sw::redis;
using namespace std;

namespace BGPRedisSaver {
    bool stop = false;
    string redis_host="127.0.0.1";
    int redis_port=6379;
    sw::redis::Redis redis = sw::redis::Redis("tcp://127.0.0.1:6379");

    void init_connections() {
        cout << "Connexion à la base Redis...";
        try {
            ConnectionOptions connection_options;
            connection_options.host = redis_host;
            connection_options.port = redis_port;
            redis = Redis(connection_options);
            cout << "done." << endl;
        } catch (const Error &e) {
            cout << "echec." << endl;
            cerr << e.what() << endl;
            exit(0);
        }

        BGPCassandraInserter::init_connections();
    }

    void end_connections() {
        BGPCassandraInserter::end_connections();
    }

    void run(vector<Ensemble> sets) {
        if(sets.empty()) {
            cerr << "BGPRedisServer::run received empty vector" << endl;
            return;
        }
        init_connections();

        int i=0;
        while (!stop) {
            string keys_set_name = sets[i].getKeys();
            string values_set_name = sets[i].getValues();
            int set_size = sets[i].getSize();
            int nb_to_del = getStructSize(keys_set_name) - set_size;

            cout << keys_set_name << " :" << endl;
            cout << "\t-size: " << set_size << endl;
            cout << "\t-nb_to_del: " << nb_to_del << endl;

            // Trouver les données à transférer
            unordered_map<string, double> data; //Recherche de la donnée à supprimer
            if(nb_to_del>0) {
                redis.zrange(keys_set_name, 0, nb_to_del, inserter(data, data.begin()));
            }

            // Parcourir ces données
            for(auto key : data) {
                // Récupérer la donnée à transférer
                const char *old_key = key.first.c_str();
                int old_timestamp = key.second;
                const char *old_value = getOldValue(values_set_name, old_key);
                vector<ValueToSave> toSave;
                getOldValues(values_set_name, old_key, toSave);

                //Supprimer la donnée de l'ensemble Redis
                redis.zremrangebyrank(keys_set_name, 0, 0); //Suppression de la clé dans l'ensemble Redis
                redis.del(old_key); //Suppression du couple clé-valeur dans la base Redis

                //Ajouter la donnée à Cassandra
                BGPCassandraInserter::insert(keys_set_name, old_key, old_value, old_timestamp);

                //TODO Ajouter un indicateur de la suppression dans Redis


            }
            i = (i+1) % sets.size();
            sleep(2);
        }

        end_connections();
    }

    void stopTransfer() {
        stop = true;
    }

    void setRedis(std::string redis_host, int redis_port) {
        BGPRedisSaver::redis_host = redis_host;
        BGPRedisSaver::redis_port = redis_port;
    }

    void setCassandra(std::string cassandra_host, int cassandra_port) {
        BGPCassandraInserter::setCassandra(cassandra_host, cassandra_port);
    }

    const char *getOldValue(string set, string key) {
        string type = redis.type(set);
        if(type=="string") {
            Optional<string> opt_res = redis.get(key);
            return opt_res->c_str();
        } else if(type=="list") {
            return "";
        } else if(type=="set") {
            return "";
        } else if(type=="zset") {
            return "";
        } else if(type=="hash") {
            Optional<string> opt_res = redis.hget(set, key);
            return opt_res->c_str();
        } else { //stream
            return "";
        }
    }

    void getOldValues(string values_set_name, string key, vector<ValueToSave> toSave) {
        string type = redis.type(values_set_name);
        if(type=="string") {
            Optional<string> opt_res = redis.get(key);
            toSave.push_back(ValueToSave(opt_res->c_str(), ValueToSave::SAVE_UNKNOWN));
            return;
        } else if(type=="list") {
            return; //TODO
        } else if(type=="set") {
            return; //TODO
        } else if(type=="zset") {
            return; //TODO
        } else if(type=="hash") {
            Optional<string> opt_res = redis.hget(values_set_name, key);
            //int type = (values_set_name.substr(0, 4)=="ASR:") ? ValueToSave::SAVE_ASEvent : ValueToSave::SAVE_RoutingEvent;
            int type = ValueToSave::SAVE_Path;
            toSave.push_back(ValueToSave(key, opt_res->c_str(), type));
            return;
        } else { //stream
            return; //TODO
        }
    }

    int getStructSize(string keys_set_name) {
        string type = redis.type(keys_set_name);
        if(type=="string") {
            return 1;
        } else if(type=="list") {
            return redis.llen(keys_set_name);
        } else if(type=="set") {
            return redis.scard(keys_set_name);
        } else if(type=="zset") {
            return redis.zcount(keys_set_name, UnboundedInterval<double>{});
        } else if(type=="hash") {
            return redis.hlen(keys_set_name);
        } else { //stream
            return redis.xlen(keys_set_name);
        }
    }

}