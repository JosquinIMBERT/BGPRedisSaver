#include <iostream>
#include <vector>
#include <signal.h>
#include <boost/algorithm/string.hpp>

#include "test.h"
#include "Ensemble.h"
#include "BGPRedisSaver.h"

using namespace std;

void sigint_handler(int signum) {
    BGPRedisSaver::stopTransfer();
}

void usage() {
    cout << "BGPRedisServer :" << endl;
    cout << endl << "\tSupprimer périodiquement des données dans une base Redis et les transférer vers une base Cassandra." << endl;
    cout << "\tPour cela, vous clés Redis doivent être sauvegadées dans un ensemble avec un score associé (Exemple : timestamp du dernier accès)." << endl;
    cout << endl << "\tOptions :" << endl;
    cout << "\t\t -R <host> <port> \t\t\t\t\t Informations de connexion à la base de données Redis" << endl;
    cout << "\t\t -C <host> <port> \t\t\t\t\t Informations de connexion à la base de données Cassandra" << endl;
    cout << "\t\t -S <keys,values,taille> [keys,values,taille ...] \t Noms des ensembles et taille maximale associée dans la base de données Redis à transférer" << endl;
    cout << "\t\t --help \t\t\t\t\t\t\t Afficher cette page d'aide" << endl;
}

bool est_option_valide(string cmd) {
    bool estValide = cmd=="-R" || cmd=="-C" || cmd=="-S";
    return estValide;
}

int main(int argc, char **argv) {
    const int MAX_SIZE = 150;

    vector<Ensemble> sets;
    //sets.push_back(Ensemble("PREFIXES", "", MAX_SIZE));
    sets.push_back(Ensemble("APATHS", "PATHS", MAX_SIZE));
    //sets.push_back(Ensemble("INACTIVEPATHS", "", MAX_SIZE));
    //sets.push_back(Ensemble("ROUTINGENTRIES", "", MAX_SIZE));
    //sets.push_back(Ensemble("INACTIVEROUTINGENTRIES", "", MAX_SIZE));

    if(argc >= 2) {
        string argv1(argv[1]); if(argv1=="test") {test::test(); return 0;}
        for(int i=1; i<argc; i++) {
            string cmd(argv[i]);
            if(cmd=="--help" || !est_option_valide(cmd)) {
                usage();
                return 0;
            } else {
                if(cmd=="-R") { //Données de connexion Redis
                    string redis_host = argv[++i];
                    int redis_port = atoi(argv[++i]);
                    BGPRedisSaver::setRedis(redis_host, redis_port);
                } else if(cmd=="-C") { //Données de connexion Cassandra
                    string cassandra_host = argv[++i];
                    int cassandra_port = atoi(argv[++i]);
                    BGPRedisSaver::setCassandra(cassandra_host, cassandra_port);
                } else { //Liste des ensembles
                    sets.clear();
                    i++;
                    cmd = argv[i];
                    while(i<argc && !est_option_valide(argv[i])) {
                        cmd = argv[i];
                        vector<string> splited_cmd;
                        boost::split(splited_cmd, cmd, [](char c){return c == ',';});
                        if(splited_cmd.size()!=3) continue;
                        string keys = splited_cmd[0];
                        string values = splited_cmd[1];
                        int taille = atoi(splited_cmd[1].c_str());
                        i++;
                        sets.push_back(Ensemble(keys, values, taille));
                    }
                    if(i<argc) i--;
                }
            }
        }
    }

    signal(SIGINT, sigint_handler);

    BGPRedisSaver::run(sets);

    return 0;
}
