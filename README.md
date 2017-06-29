# Overview of CAPI-RowCache

This project extends the RowCache of [Apache Cassandra](http://cassandra.apache.org) with CAPI-Flash. All of the cached data are stored in large flash devices through a high-throughput low-latency FPGA-based accelerator, [CAPI-Flash](https://www.ibm.com/power/solutions/bigdata-analytics-data-engine-nosql). CAPI-RowCache is a plug-in jar to Apache Cassandra. On a POWER Linux machine with CAPI-Flash installed, CAPI-RowCache is enabled by placing [capi-rowcache.jar](https://github.com/hhorii/capi-rowcache/releases/download/v0.1/capi-rowcache.jar) and associated [capiblock.jar](https://github.com/hhorii/capi-rowcache/releases/download/v0.1/capiblock.jar) to Cassandra's `lib` directory and by specifying necessary properties to the JVM.

## CAPI-Flash (also known as IBM Data Engine for NoSQL)

CAPI-Flash provides high-throughput low-latency access to flash storage. With the help of POWER8's [CAPI](http://www-304.ibm.com/webapp/set2/sas/f/capi/home.html) (Coherent Accelerator Processor Interface) capability, a CAPI-Flash card directly accesses the main memory POWER8 processors are using. This design simplifies and optimizes the data exchanges between the main memory and flash storage. CAPI-Flash doesn't require processing in OS. No OS intervention reduces overheads to access the flash storage.

> IBM Data Engine for NoSQL is an integrated platform for large and fast growing NoSQL data stores. It builds on the CAPI capability of POWER8 systems and provides super-fast access to large flash storage capacity. It delivers high speed access to both RAM and flash storage which can result in significantly lower cost, and higher workload density for NoSQL deployments than a standard RAM-based system. The solution offers superior performance and price-performance to scale out x86 server deployments that are either limited in available memory per server or have flash memory with limited data access latency.
> -- [IBM Data Engine for NoSQL](https://www.ibm.com/power/solutions/bigdata-analytics-data-engine-nosql)

## CAPI-RowCache for Apache Cassandra

Apache Cassandra is an open source, non-relational, horizontally scalable, distributed database management system. When serving thousands of read operations per second per node, Cassandra is typically bottlenecked by disk I/O. The read path of Cassandra can include an in-memory cache, called RowCache, but when the cache misses, Cassandra must read the data from disks. These disk I/O activities suffer from high latency. Even if flash SSDs are used instead of spinning disks, there still remains overhead in the file system and the device driver.

CAPI-RowCache naturally extends the original in-memory RowCache mechanism to high-throughput low-latency CAPI-Flash. You no longer need large expensive DRAM to cache your data in memory. Instead, you can exploit inexpensive flash storage but still do not suffer from the overhead in the OS. CAPI-RowCache is to optimize read-intensive workloads. We will soon release another CAPI-Flash exploitation for Apache Cassandra to optimize write-intensive workloads.

To run CAPI-RowCache at its full speed, you need a POWER Linux machine with a CAPI-Flash card, but we also provide an emulation mode in which CAPI-Flash is emulated by a regular file on a file system. Using the emulation mode, you can try CAPI-RowCache on a POWER Linux machine without a CAPI-Flash card or even on an x86 Linux machine.

## Download

Go to the [release page](https://github.com/hhorii/capi-rowcache/releases) and download the latest capi-rowcache.jar and capiblock.jar.

## How to run

CAPI-RowCache was tested on Cassandra 3.10. It should work with other 3.x releases, too. If you need help in running CAPI-RowCache on other versions of Cassandra, please raise an issue.

This section explains how to run CAPI-RowCache with a machine with CAPI-Flash. The next section describes how to run CAPI-RowCache in the emulation mode.

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
-Dcom.ibm.capiflash.cassandra.cache.devices=/dev/sg0:0:512
```

This means that your CAPI-Flash device is /dev/sg0, the start address of CAPI-RowCache is 0 in the flash address space, and the size of CAPI-RowCache is 512 GB. You can find the CAPI-Flash device on your POWER Linux machine by executing the /opt/ibm/capikv/bin/cxlfstatus command. More detailed description about this property can be found in [this page](https://github.com/hhorii/capi-rowcache/wiki/CAPI-RowCache-Wiki).

6. (Optional) If you run [Yahoo! Cloud Serving (System) Benchmark](https://github.com/brianfrankcooper/YCSB) to test CAPI-RowCache, you may want to specify the following property to the JVM for better caching behavior.

```
-Dcapi.hash=org.apache.cassandra.cache.capi.YCSBKeyHashFunction
```

## How to run with CAPI-Flash emulation

We provide an emulation mode in which CAPI-Flash is emulated by a regular file on a file system, so that you can try the functionality of CAPI-RowCache on a POWER Linux machine without a CAPI-Flash card or even on an x86 Linux machine.

1. Copy the downloaded jar files to Cassandra's lib directory.

```
$ cp /path/to/capi-rowcache.jar /path/to/capiblock.jar /path/to/your/cassandra/lib
```

2. Add the following line to Cassandra's conf/cassandra.yaml file.

```
row_cache_class_name: org.apache.cassandra.cache.CapiRowCacheProvider
```

3. Specify the following properties to the JVM. Usually, you specify them in Cassandra's conf/jvm.options file.

```
-Dcom.ibm.capiflash.cassandra.cache.devices=/path/to/your/capiflash/emulation/file.dat:0:10
```

This means that you will use `/path/to/your/capiflash/emulation/file.dat` to emulate CAPI-Flash. You can specify any regular file. You may want to use a file on tmpfs or `/dev/shm`. `0` after the first `:` means CAPI-RowCache starts at offset 0 in the file, and `10` after the second `:` means CAPI-RowCache will use 10 GB in the file.

```
-Dcom.ibm.research.capiblock.emulation=true
```

This property enables the CAPI-Flash emulation.

```
-Dcom.ibm.research.capiblock.capacity=2621440
```

This property specifies the size (in 4-KB blocks) of the regular file used for the emulation. It must be larger than the offset plus the size specified in the `com.ibm.capiflash.cassandra.cache.devices` property. In this example, `2621440` is equal to 2621440 * 4 KB = 10 GB. Note that this means you will create a 10-GB file on your file system.

4. (Optional) If you run [Yahoo! Cloud Serving (System) Benchmark](https://github.com/brianfrankcooper/YCSB) to test CAPI-RowCache, you may want to specify the following property to the JVM for better caching behavior.

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
