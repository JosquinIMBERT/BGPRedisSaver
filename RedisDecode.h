//
// Created by josqu on 02/07/2021.
//
#include <vector>


#ifndef BGPREDISSAVER_REDISDECODE_H
#define BGPREDISSAVER_REDISDECODE_H

namespace RedisDecode {
    unsigned int from_myencoding(std::string str);
    void from_myencodingPath(std::string str, std::vector<unsigned int> &vect);
    std::string from_myencodingPref(std::string str);

    void routingEventFromRedis(std::string str,
                   std::string *pathHash,
                   std::string *status,
                   std::string *active,
                   long unsigned int *time);
    void routingEventFromRedisKey(std::string str,
                                  std::string *prefixID,
                                  std::string *prefix,
                                  int *peer);

    void ASEventFromRedis(std::string str, std::string *prefixID, std::string *active, long unsigned int *time);
    void ASEventFromRedisKey(std::string str, std::string *dstAS, int *ASN);

    void pathFromRedis(std::string str,
                       std::string *hash,
                       std::string *collector,
                       std::vector<std::string> *path,
                       int *pathLength,
                       int *prefNum,
                       long unsigned int *lastChange,
                       double *meanUp,
                       double *meanDown,
                       bool *active);

    std::vector<std::string> split(std::string str);
}

#endif //BGPREDISSAVER_REDISDECODE_H
