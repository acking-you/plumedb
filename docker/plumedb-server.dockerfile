FROM debian:testing-slim 

# set workspace
WORKDIR /plumedb

COPY ./target/release/plumedb-server .
COPY ./target/release/plumedb-server.sh .

RUN chmod +x ./plumedb-server
RUN chmod +x ./plumedb-server.sh

# default port is 8080
ENV PLUMEDB_PORT=8080

ENV PLUMEDB_DATA="/plumedb/data"

VOLUME [${PLUMEDB_DATA}]

EXPOSE $PLUMEDB_PORT

ENTRYPOINT [ "./plumedb-server.sh"]