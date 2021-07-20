#include <iostream>
#include <vector>
#include <signal.h>
#include <thread>
#include <boost/algorithm/string.hpp>
#include <sw/redis++/redis++.h>

#include "test.h"
#include "Ensemble.h"
#include "BGPRedisSaver.h"
#include "BGPCassandraInserter.h"

using namespace sw::redis;
using namespace std;

int nb_threads = 1;
vector<thread*> threads;
vector<BGPRedisSaver*> savers;

void usage() {
    cout << "BGPRedisServer :" << endl;
    cout << endl << "\tSupprimer périodiquement des données d'une base Redis et les transférer vers une base Cassandra." << endl;
    cout << "\tPour cela, vos clés Redis doivent être sauvegadées dans un ensemble trié (ZSet) avec un score associé (Exemple : timestamp du dernier accès)." << endl;
    cout << endl << "\tOptions :" << endl;
    cout << "\t\t -R <host> <port> \t\t\t\t\t Informations de connexion à la base de données Redis" << endl;
    cout << "\t\t -C <host> <port> \t\t\t\t\t Informations de connexion à la base de données Cassandra" << endl;
    cout << "\t\t -P <bool> \t\t\t\t\t\t\t Autoriser ou non les affichages" << endl;
    cout << "\t\t -N <BATCH_MAX_SIZE> \t\t\t\t Nombre de valeurs accumulées avant l'insertion effective dans Cassandra" << endl;
    cout << "\t\t -B <sleep> \t\t\t\t\t\t Entier définissant le temps de pause entre l'analyse de 2 ensembles" << endl;
    cout << "\t\t -T <threads> \t\t\t\t\t\t Entier représentant le nombre de threads que l'on souhaite utiliser" << endl;
    cout << "\t\t --help \t\t\t\t\t\t\t Afficher cette page d'aide" << endl;
    cout << "\t\t -S <set1> [<set2> [<set3> [...]]] \t <set> correspond à <keys,values,size[,static[,dstTable]]> avec :" << endl;
    cout << "\t\t\t\t -keys: Nom de l'ensemble des clés." << endl;
    cout << "\t\t\t\t -values: Nom de l'ensemble des valeurs." << endl;
    cout << "\t\t\t\t -size: taille limite de l'ensemble (en nombre de clés)." << endl;
    cout << "\t\t\t\t -static: booléen." << endl;
    cout << "\t\t\t\t\t - mode statique : Une clé est associée à une valeur. On récupère des clés dans l'ensemble des clés et on supprime la valeur associée dans l'ensemble des valeurs." << endl;
    cout << "\t\t\t\t\t - mode non statique : les valeurs à supprimer sont des listes de valeurs. Dans ce mode, les clés de l'ensemble des clés seront concaténées avec le préfixe donné dans le champ 'values' (Ex: 'PRE')." << endl;
    cout << "\t\t\t\t -dstTable: table Cassandra dans laquelle seront transférées les valeurs." << endl;
}

bool est_option_valide(string cmd) {
    bool estValide = boost::iequals(cmd,"-R")
            || boost::iequals(cmd,"-C")
            || boost::iequals(cmd,"-S")
            || boost::iequals(cmd,"-P")
            || boost::iequals(cmd,"-B")
            || boost::iequals(cmd, "-N")
            || boost::iequals(cmd, "-T");
    return estValide;
}

void wait_end(string redis_host, int redis_port) {
    //Connexion à Redis pour le message de fin
    Redis redis = Redis("tcp://127.0.0.1:6379");
    try {
        ConnectionOptions connection_options;
        connection_options.host = redis_host;
        connection_options.port = redis_port;
        redis = Redis(connection_options);
    } catch (const Error &e) {
        cerr << e.what() << endl;
        exit(0);
    }

    //Abonnement au channel devant recevoir le message de fin
    auto sub = redis.subscriber();
    sub.on_message([](std::string channel, std::string msg) {
        if(boost::iequals(channel, BGPRedisSaver::CHANNEL_END) && boost::iequals(msg,"stop")) {
            BGPRedisSaver::stop = true;
        }
    });
    sub.subscribe(BGPRedisSaver::CHANNEL_END);

    //Attente de la publication du message de fin
    while(!BGPRedisSaver::stop) {
        try {
            sub.consume();
        } catch (const Error &err) {
            cerr << "Pub/Sub error, ending thread" << endl;
            exit(EXIT_FAILURE);
        }
    }
}

void launcher(int i) {
    savers.at(i)->run();
}

int main(int argc, char **argv) {
    const int MAX_SIZE = 496460;

    string redis_host = "127.0.0.1";
    int redis_port = 6379;

    string cassandra_host = "127.0.0.1";
    int cassandra_port = 9042;

    bool print = false;
    int sleep_duration = 0;
    int batch_max_size = 1000;

    vector<Ensemble> sets;
    sets.push_back(Ensemble("APATHS", "PATHS", MAX_SIZE, true, "PATH"));
    sets.push_back(Ensemble("ROUTINGENTRIES", "PRE", 3032520, false, "ROUTINGEVENT"));

    if(argc >= 2) {
        string argv1(argv[1]); if(boost::iequals(argv1,"test")) {test::test(); return 0;}
        for(int i=1; i<argc; i++) {
            string cmd(argv[i]);
            if(boost::iequals(cmd,"--help") || !est_option_valide(cmd)) { //Afficher l'aide
                usage();
                return 0;
            } else {
                if(boost::iequals(cmd,"-R")) { //Données de connexion Redis
                    redis_host = argv[++i];
                    redis_port = atoi(argv[++i]);
                } else if(boost::iequals(cmd,"-C")) { //Données de connexion Cassandra
                    cassandra_host = argv[++i];
                    cassandra_port = atoi(argv[++i]);
                } else if(boost::iequals(cmd,"-P")) { //Print (boolean)
                    string str_bool(argv[++i]);
                    print = boost::iequals(str_bool,"true")
                            || boost::iequals(str_bool,"1")
                            || boost::iequals(str_bool, "T");
                } else if(boost::iequals(cmd,"-B")) { //Break duration
                    sleep_duration = atoi(argv[++i]);
                } else if(boost::iequals(cmd, "-N")) { //Nombre de commandes accumulables (BATCH_SIZE)
                    batch_max_size = atoi(argv[++i]);
                } else if(boost::iequals(cmd, "-T")) { //Nombre de threads
                    nb_threads = atoi(argv[++i]);
                } else { //Liste des ensembles
                    sets.clear();
                    i++;
                    cmd = argv[i];
                    while(i<argc && !est_option_valide(argv[i])) {
                        cmd = argv[i];
                        vector<string> splited_cmd;
                        boost::split(splited_cmd, cmd, [](char c){return c == ',';});
                        if(splited_cmd.size()<3) continue;
                        string keys = splited_cmd[0];
                        string values = splited_cmd[1];
                        int taille = atoi(splited_cmd[2].c_str());
                        i++;
                        bool isStatic = true;
                        string dstTable = "";
                        if(splited_cmd.size()>=4) {
                            string str_static(splited_cmd[3]);
                            isStatic = boost::iequals(str_static,"true")
                                    || boost::iequals(str_static,"1")
                                    || boost::iequals(str_static, "T");
                            if(splited_cmd.size()>=5)
                                dstTable = splited_cmd[4];
                        }
                        sets.push_back(Ensemble(keys, values, taille, isStatic, dstTable));
                    }
                    if(i<argc) i--;
                }
            }
        }
    }

    if(print) {
        BGPRedisSaver::mtx_print->lock();
        cout << "sets(" << sets.size() << ") :" << endl;
        for(auto set : sets) {
            cout << "\t" << set.toString() << endl;
        }
        BGPRedisSaver::mtx_print->unlock();
    }

    savers = vector<BGPRedisSaver*>(nb_threads);
    threads = vector<thread*>(nb_threads);
    for(int i=0; i<nb_threads; i++) {
        savers[i] = new BGPRedisSaver(i,
                                       print,
                                       sleep_duration,
                                       batch_max_size,
                                       redis_host,
                                       redis_port,
                                       cassandra_host,
                                       cassandra_port,
                                       &sets);
        threads[i] = new thread(launcher, i);
    }

    wait_end(redis_host, redis_port);

    for(int i=nb_threads-1; i>=0; i--) {
        threads.at(i)->join();
    }

    return 0;
}
