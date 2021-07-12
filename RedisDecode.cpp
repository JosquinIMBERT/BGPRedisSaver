//
// Created by josqu on 02/07/2021.
//
#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>

#include "RedisDecode.h"

using namespace std;

namespace RedisDecode {

    unsigned int from_myencoding(string str) {
        unsigned int val = 0, coef = 1, l;
        for (unsigned long i = 0; i < str.length(); i++) {
            if (str[i] < 58)
                l = str[i] - 33;
            else
                l = str[i] - 33 - 1;
            val = val + (unsigned char) l * coef;
            coef = 92 * coef;
        }
        return val;
    }


    void from_myencodingPath(string str, vector<unsigned int> *vect) {
        string str1;
        for(int i=0;i<str.length();i=i+4){
            str1=str.substr(i,4);
            vect->push_back(from_myencoding(str1));
        }
    }


    string from_myencodingPref(string str) {
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
        return pfxstr;
    }






    //######################### ROUTING EVENTS #########################
    void routingEventFromRedis(string str,
                               string *pathHash,
                               string *status,
                               string *active,
                               long unsigned int *time) {
        vector<string> values = split(str);
        *pathHash = values.at(0);
        *active = values.at(1);
        *time = from_myencoding(values.at(2));
        *status = values.at(3);
    }

    void routingEventFromRedisKey(string str,
                                  string *prefixID,
                                  string *prefix,
                                  int *peer) {
        vector<string> values = split(str);
        *prefixID = values.at(0); //Ignore prefix "PRE:" in key at index 0
        *prefix = from_myencodingPref(*prefixID);
        *peer = from_myencoding(values.at(1));
    }






    //######################### AS EVENTS #########################
    void ASEventFromRedis(string str, string *prefixID, string *active, long unsigned int *time) {
        vector<string> values = split(str);
        *prefixID = values.at(0);
        *active = values.at(1);
        *time = from_myencoding(values.at(2));
    }

    void ASEventFromRedisKey(string str, string *dstAS, int *ASN) {
        vector<string> values = split(str);
        *dstAS = values.at(1); //Ignore prefix "ASR:" in key at index 0
        *ASN = from_myencoding(*dstAS);
    }





    //######################### PATHS #########################
    void pathFromRedis(string str,
                       string *hash,
                       string *collector,
                       vector<string> *path,
                       int *pathLength,
                       int *prefNum,
                       long unsigned int *lastChange,
                       double *meanUp,
                       double *meanDown,
                       bool *active) {
        vector<string> values = split(str);
        *hash = to_string(from_myencoding(values.at(0)));
        vector<unsigned int> vec;
        from_myencodingPath(values.at(1), &vec);
        for(auto val : vec) {path->push_back(to_string(val));}
        *pathLength = from_myencoding(values.at(2));
        *prefNum = from_myencoding(values.at(3));
        *lastChange = from_myencoding(values.at(4));
        *meanUp = from_myencoding(values.at(5));
        *meanDown = from_myencoding(values.at(6));
        *collector = to_string(from_myencoding(values.at(7)));
        *active = boost::iequals(values.at(8),"T");
    }


    vector<string> split(string str) {
        vector<string> results;
        boost::split(results,str, [](char c){return c == ':';});
        return results;
    }
}