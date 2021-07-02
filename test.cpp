//
// Created by josqu on 28/06/2021.
//
#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include "bgpstream.h"

#include "test.h"

using namespace std;

/*vector<string> fromRedis(string str){
    vector<string> results;
    vector<unsigned int> pathVect;
    results.clear();
    boost::split(results,str, [](char c){return c == ':';});
    int i=0;
    hash = from_myencoding(results[i++]);
    from_myencodingPath(results[i++], pathVect);
    shortPathLength = pathVect.size();
    shortPath = new unsigned int[shortPathLength];
    memcpy(shortPath,&pathVect[0],sizeof(unsigned int)*shortPathLength);
    pathLength = from_myencoding(results[i++]);
    prefNum = from_myencoding(results[i++]);
    lastChange  = from_myencoding(results[i++]);
    meanUp= from_myencoding(results[i++])/10000;
    meanDown = from_myencoding(results[i++])/10000;
    collector= from_myencoding(results[i++]);
    if (results[i++] == "T")
        active = true;
    else
        active = false;
}*/

unsigned int from_myencoding(string str){
    unsigned int val=0,coef=1,l;
    for(unsigned long i=0;i<str.length();i++){
        if (str[i]<58)
            l=str[i]-33;
        else
            l=str[i]-33-1;
        val = val+(unsigned char)l*coef;
        coef = 92*coef;
    }
    return val;
}

void from_myencodingPath(string str, vector<unsigned int> &vect){
    string str1;
    for(int i=0;i<str.length();i=i+4){
        str1=str.substr(i,4);
        vect.push_back(from_myencoding(str1));
    }
}

string from_myencodingPref(string str, bgpstream_pfx_t *inpfx) {
    unsigned long val=0,coef=1,l, ipval;
    unsigned int len;
    string pfxstr="";
    for(unsigned long i=0;i<str.length();i++){
        if (str[i]<58)
            l=str[i]-33;
        else
            l=str[i]-33-1;
        val += (unsigned char)l*coef;
        coef = 92*coef;
    }
    len= (uint8_t)((val & 0X000000FF00000000)>>32);
    ipval = val & 0x00000000FFFFFFFF;
    for (int i=0;i<4;i++){
        pfxstr += to_string(ipval % 256);
        if (i<3)
            pfxstr +=".";
        ipval = ipval/256;
    }
    pfxstr+="/"+to_string(len);
    //bgpstream_str2pfx(pfxstr.c_str(),inpfx);
    return pfxstr;
}

void test::test() {
    cout << "######################## Tests ########################" << endl;


    bgpstream_pfx_t prefix;
    //Ces valeurs proviennent de ROUTINGENTRIES
    cout << "La valeur \"}}c)]0:Q}%\" du ZSet ROUTINGENTRIES vaut (une fois décomposé en deux champs) :" << endl;
        cout << "\t- pfxID : " << from_myencodingPref("}}c)]0", &prefix) << endl;
        cout << "\t- peer : " << from_myencoding("Q}%") << endl;


    cout << endl;


    //Cette valeur est une clé dans ASN
    cout << "La clé \"b1(\" du HSet ASN vaut : " << from_myencoding("b1(") << endl;


    cout << endl;


    //Cette valeur est une clé dans PATH2ID
    vector<unsigned int> vec;
    from_myencodingPath("js'!9I%!)>(!#kA!", vec);
    cout << "La clé \"js'!9I%!)>(!#kA!\" du HSet PATH2ID vaut : ";
    for(auto v : vec) {
        cout << v << ", ";
    }
    cout << endl;
    //cout << from_myencodingPref("abc", NULL) << endl;




    //Cette valeur est une clé dans PATH2ID
    vec.clear();
    from_myencodingPath("@{!!7l!!^i#!;&$!", vec);
    cout << "La clé \"@{!!7l!!^i#!;&$!\" du HSet PATH2ID vaut : ";
    for(auto v : vec) {
        cout << v << ", ";
    }
    cout << endl;






    /*cout << endl << endl << endl << endl << endl;
    const auto t = chrono::system_clock::now();
    time_t time = chrono::duration_cast<chrono::seconds>(t.time_since_epoch()).count();
    long int months = time/2592000; //2 592 000 = 60*60*24*30
    long int days = (time-months*2592000)/86400, //86 400 = 60*60*24
    nanos = (time-days*86400)*1000000000;
    cout << time << endl;
    //Devrait valoir environ 1530 mois,
    cout << months << " months, " << days << " days, " << nanos << " nanos" << endl;
    cout << (months*2592000 + days*86400 + nanos/1000000000) << endl;
    */
}