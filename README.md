# Overview of CAPI-RowCache

This project extends the RowCache of Apache Cassandra with CAPI-Flash. All of the cached data are stored in large flash devices through a high-throughput low-latency FPGA-based accelerator, [CAPI-Flash](https://www.ibm.com/power/solutions/bigdata-analytics-data-engine-nosql). CAPI-RowCache is a plug-in jar to Apache Cassandra. On a POWER Linux machine with CAPI-Flash installed, CAPI-RowCache is enabled by placing [capi-rowcache.jar](https://github.com/hhorii/capi-rowcache/releases/download/v0.1/capi-rowcache.jar) and associated [capiblock.jar](https://github.com/hhorii/capi-rowcache/releases/download/v0.1/capiblock.jar) to Cassandra's `lib` directory and by specifying necessary properties to the JVM.

## CAPI-Flash (also known as IBM Data Engine for NoSQL)

CAPI-Flash provides high-throughput low-latency access to flash storage. With the help of POWER8's [CAPI](http://www-304.ibm.com/webapp/set2/sas/f/capi/home.html) (Coherent Accelerator Processor Interface) capability, a CAPI-Flash card directly accesses the main memory POWER8 processors are using. This design simplifies and optimizes the data exchanges between the main memory and flash storage. CAPI-Flash doesn't require processing in OS. No OS intervention reduces overheads to access the flash storage.

> IBM Data Engine for NoSQL is an integrated platform for large and fast growing NoSQL data stores. It builds on the CAPI capability of POWER8 systems and provides super-fast access to large flash storage capacity. It delivers high speed access to both RAM and flash storage which can result in significantly lower cost, and higher workload density for NoSQL deployments than a standard RAM-based system. The solution offers superior performance and price-performance to scale out x86 server deployments that are either limited in available memory per server or have flash memory with limited data access latency.
> -- [IBM Data Engine for NoSQL](https://www.ibm.com/power/solutions/bigdata-analytics-data-engine-nosql)

## CAPI-RowCache for Apache Cassandra

[Apache Cassandra](http://cassandra.apache.org) is an open source, non-relational, horizontally scalable, distributed database management system. When serving thousands of read operations per second per node, Cassandra is typically bottlenecked by disk I/O. The read path of Cassandra can include an in-memory cache, called RowCache, but when the cache misses, Cassandra must read the data from disks. These disk I/O activities suffer from high latency. Even if flash SSDs are used instead of spinning disks, there still remains overhead in the file system and the device driver.

CAPI-RowCache naturally extends the original in-memory RowCache mechanism to high-throughput low-latency CAPI-Flash. You no longer need large expensive DRAM to cache your data in memory. Instead, you can exploit inexpensive flash storage but still do not suffer from the overhead in the OS. CAPI-RowCache is to optimize read-intensive workloads. We will soon release another CAPI-Flash exploitation for Apache Cassandra to optimize write-intensive workloads.

## Download

Go to the [release page](https://github.com/hhorii/capi-rowcache/releases) and download the latest capi-rowcache.jar and capiblock.jar.

## How to run

CAPI-RowCache was tested on Cassandra 3.11. It should work with other 3.x releases, too. If you need help in running CAPI-RowCache on other versions of Cassandra, please raise an issue.

1. Install CAPI-Flash. This page does not cover how to install CAPI-Flash.  Follow the instructions in the CAPI-Flash manual.

2. Set the LD_LIBRARY_PATH environment variable to include the library directory of your CAPI-Flash installation (usually /opt/ibm/capikv/lib).

3. Copy the downloaded jar files to Cassandra's lib directory.

```
$ cp /path/to/capi-rowcache.jar /path/to/capiblock.jar /path/to/your/cassandra/lib
```

4. Add the following line to Cassandra's conf/cassandra.yaml file.

```
row_cache_class_name: org.apache.cassandra.cache.CapiRowCacheProvider
```

5. Specify the following property to the JVM. Usually, you specify it in Cassandra's conf/jvm.options file.

```
-Dcapi.devices=/dev/sg0:0:512
```

This means that your CAPI-Flash device is /dev/sg0, the start address of CAPI-RowCache is 0 in the flash address space, and the size of CAPI-RowCache is 512GB. You can find the CAPI-Flash device on your POWER Linux machine by executing the /opt/ibm/capikv/bin/cxlfstatus command. More detailed explanation about how to specify the `capi.devices` property can be found in this page.

6. (Optional) If you run [Yahoo! Cloud Serving (System) Benchmark](https://github.com/brianfrankcooper/YCSB) to test CAPI-RowCache, you may want to specify the following property to the JVM for better caching behavior.

```
-Dcapi.hash=org.apache.cassandra.cache.capi.YCSBKeyHashFunction
```

## How to build

If you want to build CAPI-RowCache from source, follow these steps.

1. Clone this project.

```
$ git clone https://github.com/hhorii/capi-rowcache
```

2. Initialize and load submodules.

```
$ cd capi-rowcache
$ git submodule init
$ git submodule update
```

3. Call ant.

```
$ ant
```

4. Find `capi-rowcache.jar` generated in the `dist` directory.
