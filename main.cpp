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
    cout << "\tPour cela, vos clés Redis doivent être sauvegadées dans un ensemble trié (ZSet) avec un score associé (Exemple : timestamp du dernier accès)." << endl;
    cout << endl << "\tOptions :" << endl;
    cout << "\t\t -R <host> <port> \t\t\t\t\t Informations de connexion à la base de données Redis" << endl;
    cout << "\t\t -C <host> <port> \t\t\t\t\t Informations de connexion à la base de données Cassandra" << endl;
    cout << "\t\t -P <bool> \t\t\t\t\t\t\t Autoriser ou non les affichages" << endl;
    cout << "\t\t -S <keys,values,taille[,static,dstTable]> [keys,values,taille ...]" << endl;
    cout << "\t\t\t -keys: Nom de l'ensemble des clés." << endl;
    cout << "\t\t\t -values: Nom de l'ensemble des valeurs." << endl;
    cout << "\t\t\t -taille: taille limite de l'ensemble (en nombre de clés)." << endl;
    cout << "\t\t\t -static: booléen." << endl;
    cout << "\t\t\t\t - mode statique : Une clé est associée à une valeur. On récupère des clés dans l'ensemble des clés et on supprime la valeur associée dans l'ensemble des valeurs." << endl;
    cout << "\t\t\t\t - mode non statique : les valeurs à supprimer sont des listes de valeurs. Dans ce mode, les clés de l'ensemble des clés seront concaténées avec le préfixe donné dans le champ 'values' (Ex: 'PRE')." << endl;
    cout << "\t\t\t -dstTable: table Cassandra dans laquelle seront transférées les valeurs." << endl;
    cout << "\t\t -B <sleep> \t\t\t\t\t\t Entier définissant le temps de pause entre l'analyse de 2 ensembles" << endl;
    cout << "\t\t --help \t\t\t\t\t\t\t Afficher cette page d'aide" << endl;
}

bool est_option_valide(string cmd) {
    bool estValide = cmd=="-R" || cmd=="-C" || cmd=="-S" || cmd=="-P" || cmd=="-B";
    return estValide;
}

int main(int argc, char **argv) {
    const int MAX_SIZE = 496460;

    vector<Ensemble> sets;
    //sets.push_back(Ensemble("PREFIXES", "", MAX_SIZE));
    sets.push_back(Ensemble("APATHS", "PATHS", MAX_SIZE, true, "PATH"));
    //sets.push_back(Ensemble("INACTIVEPATHS", "", MAX_SIZE));
    sets.push_back(Ensemble("ROUTINGENTRIES", "PRE", 3032520, false, "ROUTINGEVENT"));
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
                } else if(cmd=="-P") {
                    string str_bool(argv[++i]);
                    bool print = str_bool=="true" || str_bool=="1";
                    BGPRedisSaver::setPrint(print);
                } else if(cmd=="-B") {
                    BGPRedisSaver::setSleepDuration(atoi(argv[++i]));
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
                            isStatic = str_static=="true" || str_static=="1";
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

    cout << "sets(" << sets.size() << ") :" << endl;
    for(auto set : sets) {
        cout << "\t" << set.toString() << endl;
    }

    signal(SIGINT, sigint_handler);

    BGPRedisSaver::run(sets);

    return 0;
}
