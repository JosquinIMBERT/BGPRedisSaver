# Documentation for the BGPRedisSaver application






## Table of contents <a name="table"></a>
1. [Table of contents](#table)
2. [Overview](#intro)
3. [Before you start](#start)
   1. [Create Cassandra Tables & Keyspace](#create_cassandra)
   2. [Install Libraries](#libraries)
   3. [Edit CMakeLists for your own infrastructure](#cmakelists)
4. [Use the application](#use)






## Overview <a name="intro"></a>

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.






## Before you start <a name="start"></a>

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.



### Servers <a name="servers"></a>
Before you start to use the application, you have to ensure that you have both Redis AND Cassandra servers running (you can use remote servers).

Redis :
   * Installation : https://redis.io/download
   * Running :
      * Linux : sudo service redis-server start
      * Windows : Go to the directory where you installed Redis (usually /Redis) and run redis-server.exe

Cassandra :
   * Installation : https://cassandra.apache.org/doc/latest/getting_started/installing.html
   * Running : Go to the directory where you installed Cassandra (usually "Program Files"\apache-cassandra-3.11.10\bin) and run "cassandra.bat -f"


### Create Cassandra Tables & Keyspace <a name="create_cassandra"></a>
    cqlsh --file="path\to\BGPCassandraDatabaseCreation"


### Install Libraries <a name="librairies"></a>
   You must also ensure that you installed the following libraries :

   * Redis++ : https://github.com/sewenew/redis-plus-plus
   * DataStax's c++ Cassandra Driver : https://github.com/datastax/cpp-driver


### Edit CMakeLists for your own infrastructure <a name="cmakelists"></a>
   I had issues to include the DataStax c++ driver for Cassandra. You will probably have to edit the CMakeLists.txt file and to provide an access to this library.





## Use the application <a name="use"></a>

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
