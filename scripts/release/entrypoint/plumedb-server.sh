#!/bin/bash

# check `PLUMEDB_PORT`
if [ -z "$PLUMEDB_PORT" ]; then
  echo "Error: PLUMEDB_PORT  is not set."
  exit 1
fi

if [ -z "$PLUMEDB_DATA" ]; then
  echo "Error: PLUMEDB_DATA  is not set."
  exit 1
fi

# the compaction strategy 'leveled' is not selectable
./plumedb-server -s "[::]:$PLUMEDB_PORT" -l $PLUMEDB_DATA leveled