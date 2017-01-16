#!/bin/sh

set -x

CAPI=capisim
CASSANDRA=cassandra

# setup
cp $CAPI/capiblock.jar $CASSANDRA/lib/
cp dist/*.jar $CASSANDRA/lib/
cp conf/* $CASSANDRA/conf/
mv $CASSANDRA/lib/jna-4.0.0.jar $CASSANDRA/lib/jna-4.0.0.jar.bak
cp lib/jna*.jar $CASSANDRA/lib/

# kill if Cassandra daemon exists
ps aux | grep java | grep CassandraDaemon | awk '{print $2}' | xargs kill 2> /dev/null

# clear cassandra data
rm -rf $CASSANDRA/data/

# start Cassandra
rm -f $CASSANDRA/logs
$CASSANDRA/bin/cassandra

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

cd YCSB
bin/ycsb load cassandra2-cql -P workloads/workloadc -p operationcount=1000000000 -p recordcount=10000 -p fieldcount=10 -p fieldlength=100 -p requestdistribution=uniform -p hosts=localhost -threads 1 -s
bin/ycsb run cassandra2-cql -P workloads/workloadc -p operationcount=1000000000 -p recordcount=10000 -p fieldcount=10 -p fieldlength=100 -p requestdistribution=uniform -p hosts=localhost -threads 1 -s
