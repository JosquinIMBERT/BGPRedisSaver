cmake_minimum_required(VERSION 3.13)
project(BGPRedisSaver)

set(CMAKE_CXX_STANDARD 14)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(BGPRedisSaver main.cpp BGPRedisSaver.h BGPRedisSaver.cpp BGPCassandraInserter.cpp BGPCassandraInserter.h Ensemble.cpp Ensemble.h test.cpp test.h RedisDecode.cpp RedisDecode.h)


# <------------ add hiredis dependency --------------->
find_path(HIREDIS_HEADER hiredis)
target_include_directories(BGPRedisSaver PUBLIC ${HIREDIS_HEADER})
find_library(HIREDIS_LIB hiredis)
target_link_libraries(BGPRedisSaver ${HIREDIS_LIB})

# <------------ add redis-plus-plus dependency -------------->
find_path(REDIS_PLUS_PLUS_HEADER sw)
target_include_directories(BGPRedisSaver PUBLIC ${REDIS_PLUS_PLUS_HEADER})
find_library(REDIS_PLUS_PLUS_LIB redis++)
target_link_libraries(BGPRedisSaver ${REDIS_PLUS_PLUS_LIB})



# <------------ add cassandra dependency --------------->
find_path(CASSANDRA_HEADER apache-cassandra-3.11.10)
target_include_directories(BGPRedisSaver PUBLIC ${CASSANDRA_HEADER})

# <------------ add cassandra driver dependency -------------->
find_path(CASSANDRA_HEADER cassandra)
target_include_directories(BGPRedisSaver PUBLIC ${CASSANDRA_HEADER})
set(CASSANDRA_LIB /usr/local/lib/libcassandra.so)
#find_library(CASSANDRA_LIB cassandra) # Je ne sais pas pourquoi cette ligne ne fonctionne pas. Du coup je donne la localisation en brut
target_link_libraries(BGPRedisSaver ${CASSANDRA_LIB})