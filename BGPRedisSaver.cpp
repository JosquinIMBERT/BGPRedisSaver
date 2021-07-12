//
// Created by josqu on 24/06/2021.
//

#include <iostream>
#include <unistd.h>
#include <vector>
#include <cassandra.h>
#include <sw/redis++/redis++.h>
#include <boost/algorithm/string.hpp>

#include "BGPRedisSaver.h"
#include "BGPCassandraInserter.h"
#include "Ensemble.h"
#include "ValueToSave.h"

using namespace sw::redis;
using namespace std;

namespace BGPRedisSaver {
    bool stop = false;
    bool print = false;
    int sleep_duration = 2;
    string redis_host="127.0.0.1";
    int redis_port=6379;
    sw::redis::Redis redis = sw::redis::Redis("tcp://127.0.0.1:6379");

    void init_connections() {
        cout << "Connecting to Redis...";
        try {
            ConnectionOptions connection_options;
            connection_options.host = redis_host;
            connection_options.port = redis_port;
            redis = Redis(connection_options);
            cout << "done." << endl;
        } catch (const Error &e) {
            cout << "failed." << endl;
            cerr << e.what() << endl;
            exit(0);
        }

        BGPCassandraInserter::init_connections();
    }

    void end_connections() {
        BGPCassandraInserter::end_connections();
        cout << "Ending connection with Redis...done." << endl;
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


            printSetInfo(keys_set_name, set_size, nb_element, nb_to_del);


            if(nb_to_del>0) {
                // Trouver les données à transférer
                unordered_map<string, double> keys; //Recherche de la donnée à supprimer
                getKeysToDelete(keys_set_name, nb_to_del, &keys);

                if (keys.empty()) {
                    if(print) cout << "\t\t-Nothing to delete for the set." << endl;
                    i = (i+1) % sets.size();
                    continue;
                }

                int cpt=0;
                // Parcourir ces données
                for (auto key : keys) {
                    if(stop) {
                        cout << cpt << " keys deleted on " << nb_to_del << endl;
                        break;
                    }
                    // Récupérer la donnée à transférer
                    const char *old_key = key.first.c_str();
                    if (!sets[i].isStatic())
                        values_set_name = sets[i].getValues(old_key);
                    int old_timestamp = key.second;
                    vector<string> toSave;
                    getOldValues(values_set_name, old_key, &toSave);

                    //Supprimer la donnée de Redis
                    deleteKeys(keys_set_name, 0, 0); //Suppression de la clé dans l'ensemble Redis
                    deleteValues(values_set_name, old_key, sets[i].isStatic());

                    int success = 0;
                    //Ajouter la donnée à Cassandra
                    for(auto val : toSave) {
                        //if(BGPCassandraInserter::insert(sets[i].getDstTable(), keys_set_name, old_key, val, old_timestamp))
                        //    success++;
                        success += BGPCassandraInserter::insert(sets[i].getDstTable(), keys_set_name, old_key, val, old_timestamp);
                    }

                    printInfo("\t\t", cpt, values_set_name, old_key, toSave, success);

                    //TODO Ajouter un indicateur de la suppression dans Redis

                    cpt++;
                }
            }
            i = (i+1) % sets.size();
            if(!stop) sleep(sleep_duration);
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
        if(boost::iequals(type,"zset")) {
            redis.zrange(keys_set_name, 0, nb_to_del-1, inserter(*data, data->begin()));
        }
    }

    void getOldValues(string values_set_name, string key, vector<string> *toSave) {
        string type = redis.type(values_set_name);
        if(boost::iequals(type,"string")) {
            Optional<string> opt_res = redis.get(key);
            toSave->push_back(opt_res->c_str());
        } else if(boost::iequals(type,"list")) {
            redis.lrange(values_set_name, 0, -1, inserter(*toSave, toSave->begin()));
        } else if(boost::iequals(type,"set")) {
            redis.smembers(values_set_name, inserter(*toSave, toSave->begin()));
        } else if(boost::iequals(type,"zset")) {
            redis.zrange(values_set_name, 0,-1, inserter(*toSave, toSave->begin()));
        } else if(boost::iequals(type,"hash")) {
            Optional<string> opt_res = redis.hget(values_set_name, key);
            toSave->push_back(key + " " + opt_res->c_str());
        } else if(boost::iequals(type,"stream")) {
            //TODO
        } else { //none
        }
    }

    void deleteKeys(string keys_set_name, int start, int stop) {
        string type = redis.type(keys_set_name);
        if(boost::iequals(type,"zset")) {
            redis.zremrangebyrank(keys_set_name, start, stop);
        }
    }

    void deleteValues(string values_set_name, string old_key, bool isStatic) {
        string type = redis.type(values_set_name);
        if (isStatic) {
            // Remove from a HSet or directly from redis
            if (boost::iequals(type,"string")) {
                // Suppression du couple clé-valeur de la base Redis
                redis.del(old_key);
            } else if (boost::iequals(type,"hash")) {
                // Suppression de l'élément dans le Hash
                redis.hdel(values_set_name, old_key);
            }
        } else {
            if (boost::iequals(type,"list")) {
                // Suppression de la liste (Exemple "PRE:pfxID:peer" ou "ASR:ASN") entière
                redis.del(values_set_name);
            }
        }
    }

    int getStructSize(string keys_set_name) {
        string type = redis.type(keys_set_name);
        if(boost::iequals(type,"string")) {
            return 1;
        } else if(boost::iequals(type,"list")) {
            return redis.llen(keys_set_name);
        } else if(boost::iequals(type,"set")) {
            return redis.scard(keys_set_name);
        } else if(boost::iequals(type,"zset")) {
            return redis.zcount(keys_set_name, UnboundedInterval<double>{});
        } else if(boost::iequals(type,"hash")) {
            return redis.hlen(keys_set_name);
        } else if(boost::iequals(type,"stream")) {
            return redis.xlen(keys_set_name);
        } else { //none
            return 0;
        }
    }




    void setPrint(bool p) {
        print = p;
    }

    void setSleepDuration(int sleep) {
        sleep_duration = sleep;
    }



    void printSetInfo(string keys_set_name, int set_size, int nb_element, int nb_to_del) {
        if(print) {
            cout << endl << keys_set_name << " :" << endl;
            cout << "\t-type: " << redis.type(keys_set_name) << endl;
            cout << "\t-size: " << set_size << endl;
            cout << "\t-nb_element: " << nb_element << endl;
            cout << "\t-nb_to_del: " << ((nb_to_del>0) ? nb_to_del : 0) << endl;
            cout << "\t-values: " << ((nb_to_del<=0) ? "none" : "") << endl;
        }
    }

    void printInfo(string initial_tab, int ind, string values_set_name, string old_key, vector<string> toSave, int success) {
        if(print) {
            cout << initial_tab << "-Transfer " << ind << " :" << endl;
            cout << initial_tab << "\t-set: " << values_set_name << endl;
            cout << initial_tab << "\t-old_key: " << old_key << endl;
            cout << initial_tab << "\t-toSave: [";
            bool first = true;
            for (auto str : toSave) {
                if (first) first = false;
                else cout << ", ";
                cout << str;
            }
            cout << "]" << endl;
            cout << initial_tab << "\t-nb transfered data: " << success << endl;
        }
    }
}