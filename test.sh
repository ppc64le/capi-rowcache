#!/bin/sh

set -x

CASSANDRA=cassandra

# setup
cp lib/capiblock.jar $CASSANDRA/lib/

# kill if Cassandra daemon exists
ps aux | grep java | grep CassandraDaemon | awk '{print $2}' | xargs kill 2> /dev/null

# clear cassandra data
rm -rf $CASSANDRA/data/

# start Cassandra
rm -rf $CASSANDRA/logs
if [ `whoami` = "root" ]; then
  JVM_OPTS="-Dcapi.hash=org.apache.cassandra.cache.capi.YCSBKeyHashFunction" $CASSANDRA/bin/cassandra -R
else
  JVM_OPTS="-Dcapi.hash=org.apache.cassandra.cache.capi.YCSBKeyHashFunction" $CASSANDRA/bin/cassandra
fi

while true; do
  sleep 1
  HIT=`cat $CASSANDRA/logs/system.log | grep "Starting listening for CQL clients"`
  if [ "$HIT" = "" ]; then continue; fi
  break
done

$CASSANDRA/bin/cqlsh << EOF
create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1 };
USE ycsb;
create table usertable (
    y_id varchar primary key,
    field0 varchar,
    field1 varchar,
    field2 varchar,
    field3 varchar,
    field4 varchar,
    field5 varchar,
    field6 varchar,
    field7 varchar,
    field8 varchar,
    field9 varchar)
    with caching = { 'keys' : 'NONE', 'rows_per_partition' : 'ALL' };
exit
EOF

