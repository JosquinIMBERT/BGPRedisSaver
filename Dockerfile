FROM node:alpine
COPY . /app

#__Libraries installation__#
RUN apk update
RUN apk add git
RUN apk add make

# Hiredis library
RUN git clone https://github.com/redis/hiredis.git /usr/local/Hiredis
RUN make /usr/local/Hiredis/
RUN make /usr/local/Hiredis/ install

# Redis-plus-plus library
RUN git clone https://github.com/sewenew/redis-plus-plus.git
RUN cd /usr/local/redis-plus-plus
RUN mkdir build
RUN cd build
RUN cmake ..
RUN make
RUN make install

# Datastax's Cassandra driver
RUN git clone https://github.com/datastax/cpp-driver.git
RUN cmake
RUN make
RUN make install


WORKDIR /app
RUN cmake .
RUN make
CMD ["node","./BGPRedisSaver","-S","APATHS,PATHS,494899,T,PATH","ROUTINGENTRIES,PRE,3029319,F,RoutingEvent","-P","T","-T","6"]
