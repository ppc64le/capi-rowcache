#!/usr/bin/env bash

if [ $# -eq 2 ]; then
    cassandra_path=$1
    command_option=$2
elif [ $# -eq 1 ]; then
    if [ -z "$CASSANDRA_PATH" ]; then
	echo CASSANDRA_PATH not specified
	exit 1
    fi
    cassandra_path=$CASSANDRA_PATH
    command_option=$1
else
    echo commitlog-util.sh "<Cassandra path>" "{ -r | -d | -h }"
    exit 1
fi

java -cp `dirname $0`/../lib/capiblock.jar:`dirname $0`/build com.ibm.capiflash.cassandra.commitlog.util.Driver $cassandra_path $command_option
