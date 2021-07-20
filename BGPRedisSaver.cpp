//
// Created by josqu on 24/06/2021.
//

#include <iostream>
#include <unistd.h>
#include <vector>
#include <thread>
#include <sw/redis++/redis++.h>
#include <boost/algorithm/string.hpp>

#include "BGPRedisSaver.h"
#include "BGPCassandraInserter.h"

using namespace sw::redis;
using namespace std;





const string BGPRedisSaver::CHANNEL_END="ChannelEndBGPRedisSaver";
bool BGPRedisSaver::stop = false;
mutex* BGPRedisSaver::mtx_print = new mutex();





//################################ CONSTRUCTEURS ################################
BGPRedisSaver::BGPRedisSaver(const BGPRedisSaver &src) {
    id = src.id;
    print = src.print;
    sleep_duration = src.sleep_duration;
    BATCH_MAX_SIZE = src.BATCH_MAX_SIZE;

    redis_host=src.redis_host;
    redis_port=src.redis_port;

    ConnectionOptions connection_options;
    connection_options.host = this->redis_host;
    connection_options.port = this->redis_port;
    this->redis = Redis(connection_options);

    redis_pipe = this->redis.pipeline();

    sets = src.sets;
    cass_inserter = src.cass_inserter;
}
BGPRedisSaver::BGPRedisSaver(int i,
                            bool print,
                            int sleep_duration,
                            int BATCH_MAX_SIZE,
                            string redis_host,
                            int redis_port,
                            string cassandra_host,
                            int cassandra_port,
                            vector<Ensemble> *sets) {
    this->id = i;
    this->print = print;
    this->sleep_duration = sleep_duration;
    this->BATCH_MAX_SIZE = BATCH_MAX_SIZE;

    this->redis_host= redis_host;
    this->redis_port= redis_port;

    ConnectionOptions connection_options;
    connection_options.host = this->redis_host;
    connection_options.port = this->redis_port;
    this->redis = Redis(connection_options);

    this->redis_pipe = this->redis.pipeline();

    this->sets = sets;

    this->cass_inserter = BGPCassandraInserter(cassandra_host, cassandra_port, this->BATCH_MAX_SIZE);
}
















//################################ METHODES LIEES A LA CONNEXION ################################
void BGPRedisSaver::init_connections() {
    try {
        ConnectionOptions connection_options;
        connection_options.host = redis_host;
        connection_options.port = redis_port;
        redis = Redis(connection_options);
        redis_pipe = redis.pipeline();
        cout << "Connection to Redis: done.\n";
    } catch (const Error &e) {
        cout << "Connection to Redis: failed.\n";
        cerr << e.what() << endl;
        exit(0);
    }

    cass_inserter.init_connections();
}

void BGPRedisSaver::end_connections() {
    cass_inserter.end_connections();
    cout << "Ending connection with Redis: done.\n";
}















//################################ METHODES EXECUTEES PAR LE THREAD ################################
void BGPRedisSaver::run() {
    if(sets->empty()) {
        if(print) {
            cerr << "BGPRedisServer::run received empty list of sets\n";
        }
        return;
    }
    init_connections();

    int i=0;
    while (!stop) { //!mtx_stop->try_lock()) {
        string keys_set_name = sets->at(i).getKeys();
        string keys_type = redis.type(keys_set_name);

        int set_size = sets->at(i).getSize();
        int nb_element = getStructSize(keys_set_name, keys_type);
        int nb_to_del =  nb_element - set_size;
        if(nb_to_del > BATCH_MAX_SIZE) {
            nb_to_del = BATCH_MAX_SIZE;
        }


        printSetInfo(keys_set_name, keys_type, set_size, nb_element, nb_to_del);


        if(nb_to_del>0) {
            // Trouver les données à transférer
            unordered_map<string, double> keys; //Recherche de la donnée à supprimer
            getRemoveKeys(keys_set_name, keys_type, nb_to_del, &keys);

            int cpt=0;
            // Parcourir ces données
            for (auto key : keys) {
                // Récupérer la donnée à transférer
                const char *old_key = key.first.c_str();
                int old_timestamp = key.second;

                string values_set_name = sets->at(i).getValues(old_key);
                string values_type = redis.type(values_set_name);
                vector<string> toSave;

                getRemoveValues(values_set_name, values_type, old_key, &toSave);

                int success = 0;
                //Ajouter la donnée à Cassandra
                for(auto val : toSave) {
                    success += cass_inserter.insert(sets->at(i).getDstTable(), keys_set_name, old_key, val, old_timestamp);
                }

                printInfo("\t\t", cpt, values_set_name, old_key, toSave, success);

                cpt++;
            }
        }
        i = (i+1) % sets->size();
        sleep(sleep_duration);
    }

    end_connections();

    return;
}














//################################ METHODES DE MANIPULATION DES DONNEES REDIS ################################
void BGPRedisSaver::getRemoveKeys(string keys_set_name,
                                 string type,
                                 int nb_to_del,
                                 unordered_map<string, double> *keys) {
    if(boost::iequals(type,"zset")) { //Les clés doivent forcément être dans un ZSet
        redis_pipe.zrange(keys_set_name, 0, nb_to_del-1, true);
        redis_pipe.zremrangebyrank(keys_set_name, 0, nb_to_del-1);
        auto result = redis_pipe.exec();
        result.get(0, inserter(*keys, keys->begin()));
    }
}

void BGPRedisSaver::getRemoveValues(string values_set_name,
                                    string values_type,
                                    string old_key,
                                    vector<string> *toSave) {
    getValues(values_set_name, values_type, old_key, toSave);
    removeValues(values_set_name, values_type, old_key);
    auto reply = redis_pipe.exec();
    auto result = reply.get(0);
    switch(result.type) {
        case REDIS_REPLY_STRING:
            toSave->push_back(result.str);
            break;
        case REDIS_REPLY_INTEGER:
            toSave->push_back(to_string(result.integer));
            break;
        case REDIS_REPLY_ARRAY:
            reply.get(0, inserter(*toSave, toSave->begin()));
            break;
        default:
            break;
    }
}

void BGPRedisSaver::getValues(string values_set_name, string type, string key, vector<string> *toSave) {
    if(boost::iequals(type,"string")) {
        redis_pipe.get(key);
    } else if(boost::iequals(type,"list")) {
        redis_pipe.lrange(values_set_name, 0, -1);
    } else if(boost::iequals(type,"set")) {
        redis_pipe.smembers(values_set_name);
    } else if(boost::iequals(type,"zset")) {
        redis_pipe.zrange(values_set_name, 0,-1);
    } else if(boost::iequals(type,"hash")) {
        redis_pipe.hget(values_set_name, key);
    } else if(boost::iequals(type,"stream")) {
    } else { //none
    }
}

void BGPRedisSaver::removeValues(string values_set_name, string type, string old_key) {
    if (boost::iequals(type,"string")) { //Remove directly from redis
        redis_pipe.del(old_key);
    } else if (boost::iequals(type,"hash")) { //Remove from a Hash
        redis_pipe.hdel(values_set_name, old_key);
    } else if (boost::iequals(type,"list")) { //Remove a list
        redis_pipe.del(values_set_name);
    }
}

int BGPRedisSaver::getStructSize(string keys_set_name, string type) {
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









//################################ METHODES D'AFFICHAGE ################################
void BGPRedisSaver::printSetInfo(string keys_set_name, string type, int set_size, int nb_element, int nb_to_del) {
    if(print) {
        mtx_print->lock();
        cout << endl << keys_set_name << " :" << endl;
        cout << "\t-type: " << type << endl;
        cout << "\t-size: " << set_size << endl;
        cout << "\t-nb_element: " << nb_element << endl;
        cout << "\t-nb_to_del: " << ((nb_to_del>0) ? nb_to_del : 0) << endl;
        cout << "\t-values: " << ((nb_to_del<=0) ? "none" : "") << endl;
        mtx_print->unlock();
    }
}

void BGPRedisSaver::printInfo(string initial_tab, int ind, string values_set_name, string old_key, vector<string> toSave, int success) {
    if(print) {
        mtx_print->lock();
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
        mtx_print->unlock();
    }
}