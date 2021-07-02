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
            cerr << "BGPRedisServer::run received empty list of sets" << endl;
            return;
        }
        init_connections();

        int i=0;
        while (!stop) {
            string keys_set_name = sets[i].getKeys();
            string values_set_name = sets[i].getValues();
            int set_size = sets[i].getSize();
            int nb_element = getStructSize(keys_set_name);
            int nb_to_del =  nb_element - set_size;

            cout << keys_set_name << " :" << endl;
            cout << "\t-type: " << redis.type(keys_set_name) << endl;
            cout << "\t-size: " << set_size << endl;
            cout << "\t-nb_element: " << nb_element << endl;
            cout << "\t-nb_to_del: " << nb_to_del << endl;


            if(nb_to_del>0) {
                // Trouver les données à transférer
                unordered_map<string, double> keys; //Recherche de la donnée à supprimer
                getKeysToDelete(keys_set_name, nb_to_del, &keys);

                if (keys.empty()) {
                    cout << "No keys to delete for the set." << endl;
                    i = (i+1) % sets.size();
                    continue;
                }

                // Parcourir ces données
                for (auto key : keys) {
                    // Récupérer la donnée à transférer
                    const char *old_key = key.first.c_str();
                    if (!sets[i].isStatic())
                        values_set_name = sets[i].getValues(old_key);
                    int old_timestamp = key.second;
                    vector<string> toSave;
                    getOldValues(values_set_name, old_key, &toSave);
                    cout << "old_key="<<old_key<<", toSave=[";
                    for(auto str : toSave) {
                        cout << str << ", ";
                    } cout << "]" << endl;

                    //Supprimer la donnée de Redis
                    deleteKeys(keys_set_name, 0, 0); //Suppression de la clé dans l'ensemble Redis
                    deleteValues(values_set_name, old_key, sets[i].isStatic());

                    //Ajouter la donnée à Cassandra
                    for(auto val : toSave) {
                        //TODO seul endroit qu'il reste à modifier en théorie :
                        // passer les bonnes infos à Cassandra car la spécification de insert devrait changer
                        BGPCassandraInserter::insert(sets[i].getDstTable(), keys_set_name, old_key, val, old_timestamp);
                    }

                    //TODO Ajouter un indicateur de la suppression dans Redis


                }
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

    void getKeysToDelete(string keys_set_name, int nb_to_del, unordered_map<string, double> *data) {
        string type = redis.type(keys_set_name);
        if(type=="zset") {
            redis.zrange(keys_set_name, 0, nb_to_del, inserter(*data, data->begin()));
        }
    }

    void getOldValues(string values_set_name, string key, vector<string> *toSave) {
        string type = redis.type(values_set_name);
        if(type=="string") {
            Optional<string> opt_res = redis.get(key);
            toSave->push_back(opt_res->c_str());
        } else if(type=="list") {
            redis.lrange(values_set_name, 0, -1, inserter(*toSave, toSave->begin()));
        } else if(type=="set") {
            redis.smembers(values_set_name, inserter(*toSave, toSave->begin()));
        } else if(type=="zset") {
            redis.zrange(values_set_name, 0,-1, inserter(*toSave, toSave->begin()));
        } else if(type=="hash") {
            Optional<string> opt_res = redis.hget(values_set_name, key);
            toSave->push_back(key + " " + opt_res->c_str());
        } else if(type=="stream") {
            //TODO
        } else { //none

        }
    }

    void deleteKeys(string keys_set_name, int start, int stop) {
        string type = redis.type(keys_set_name);
        if(type=="zset") {
            redis.zremrangebyrank(keys_set_name, start, stop);
        }
    }

    void deleteValues(string values_set_name, string old_key, bool isStatic) {
        string type = redis.type(values_set_name);
        if (isStatic) {
            // Remove from a HSet or directly from redis
            if (type == "string") {
                // Suppression du couple clé-valeur de la base Redis
                redis.del(old_key);
            } else if (type == "hash") {
                // Suppression de l'élément dans le Hash
                redis.hdel(values_set_name, old_key);
            }
        } else {
            if (type == "list") {
                // Suppression de la liste (Exemple "PRE:pfxID:peer" ou "ASR:ASN") entière
                redis.del(values_set_name);
            }
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
        } else if(type=="stream") {
            return redis.xlen(keys_set_name);
        } else { //none
            return 0;
        }
    }

}