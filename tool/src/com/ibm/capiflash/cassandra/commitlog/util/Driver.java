/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.capiflash.cassandra.commitlog.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

public class Driver {
    static ArrayList<String> DEVICES;
    static long START_OFFSET;
    static int NUM_SEGMENTS;
    
    final static int BLOCK_SIZE = 4096;
    final static CapiBlockDevice dev = CapiBlockDevice.getInstance();

    private static void printUsage() {
	System.out.println("Usage: java com.ibm.capiflash.cassandra.util.Driver <Cassandra path> { -r | -d | -h }\n");
	System.out.println("  -r   read and print commit log in CAPI Flash");
	System.out.println("  -d   delete all commitlog bookkeeping segments in CAPI Flash");
	System.out.println("  -h   print this help");
    }

    public static void main(String[] args) throws IOException {
	try {
	    if (args.length != 2) {
		System.err.println("Invalid Argument");
		printUsage();
		System.exit(1);
	    }
	    if (args[1].equals("-h")) {
		printUsage();
		System.exit(0);
	    } else if (! args[1].equals("-r") && ! args[1].equals("-d")) {
		System.err.println("Invalid Argument");
		printUsage();
		System.exit(1);
	    }
	    DEVICES = new ArrayList<String>();
	    int set = 0;
	    try (BufferedReader br = new BufferedReader(new FileReader(args[0] + "/conf/cassandra.yaml"))) {
		String line;
		while ((line = br.readLine()) != null) {
		    if (line.startsWith("capiflashcommitlog_devices:")) {
			System.out.println(line);
			for ( ; ; ) {
			    line = br.readLine();
			    if (line == null || line.indexOf("- ") == -1) {
				break;
			    }
			    String DEVICE = line.split("-")[1].trim();
			    DEVICES.add(DEVICE);
			    //System.out.println(DEVICE);
			}
			set++;
		    } else if (line.startsWith("capiflashcommitlog_number_of_segments:")) {
			System.out.println(line);
			NUM_SEGMENTS = Integer.valueOf(line.split(":")[1].trim());
			//System.out.println(NUM_SEGMENTS);
			set++;
		    } else if (line.startsWith("capiflashcommitlog_start_offset:")) {
			System.out.println(line);
			START_OFFSET = Long.valueOf(line.split(":")[1].trim());
			//System.out.println(START_OFFSET);
			set++;
		    }
		}
	    }
	    if (DEVICES.size() == 0) {
		try (BufferedReader br = new BufferedReader(new FileReader(args[0] + "/conf/jvm.options"))) {
		String line;
		while ((line = br.readLine()) != null) {
		    if (line.startsWith("-Dcom.ibm.capiflash.cassandra.commitlog.devices")) {
			System.out.println(line);
			for (String DEVICE : line.split("=")[1].trim().split(",")) {
			    DEVICES.add(DEVICE);
			    //System.out.println(DEVICE);
			}
			set++;
		    } else if (line.startsWith("-Dcom.ibm.capiflash.cassandra.commitlog.number_of_segments")) {
			System.out.println(line);
			NUM_SEGMENTS = Integer.valueOf(line.split("=")[1].trim());
			//System.out.println(NUM_SEGMENTS);
			set++;
		    } else if (line.startsWith("-Dcom.ibm.capiflash.cassandra.commitlog.start_offset=")) {
			System.out.println(line);
			START_OFFSET = Long.valueOf(line.split("=")[1].trim());
			//System.out.println(START_OFFSET);
			set++;
		    }
		}
	    }
	    }
	    if (set != 3) {
		System.err.println("Either one or more of the following properties is not set in " + args[0] + "/conf/jvm.options:");
		System.err.println("com.ibm.capiflash.cassandra.commitlog.devices");
		System.err.println("com.ibm.capiflash.cassandra.commitlog.number_of_segments");
		System.err.println("com.ibm.capiflash.cassandra.commitlog.start_offset");
		System.exit(1);
	    }
	    for (String DEVICE : DEVICES) {
		Chunk chunk = dev.openChunk(DEVICE);
		if (args[1].equals("-d")) {
		    deleteAll(chunk, DEVICE);
		} else {
		    readFreeList(chunk, DEVICE);
		}
		chunk.close();
	    }

	} catch (IOException e1) {
	    e1.printStackTrace();
	    System.exit(1);
	}
    }

    private static void readFreeList(Chunk chunk, String DEVICE) throws IOException {
	ByteBuffer util = ByteBuffer.allocateDirect(BLOCK_SIZE);
	long segId;
	long empty = 0;
	long used = 0;
	for (long i = 0; i < NUM_SEGMENTS; i++) {
	    chunk.readBlock(START_OFFSET + i, 1, util);
	    segId = util.getLong();
	    util.clear();
	    if (segId == 0) {
		empty++;
	    } else {
		used++;
		//System.out.println("Block in Use ! OFFSET:" + i + " SegmentID:"
		//		   + segId);
	    }
	}
	System.out.println(empty + " empty blocks");
	System.out.println(used + " used blocks");
    }

    private static void deleteAll(Chunk chunk, String DEVICE) throws IOException {
	ByteBuffer util = ByteBuffer.allocateDirect(BLOCK_SIZE);
	util.putLong(0);
	for (long i = 0; i < NUM_SEGMENTS; i++) {
	    chunk.writeBlock(START_OFFSET + i, 1, util);
	}
	System.out.println("Freelist Blocks at " + START_OFFSET + " cleared on " + DEVICE);
    }

}
