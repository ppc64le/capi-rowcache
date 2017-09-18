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
package com.ibm.capiflash.cassandra.commitlog;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;

class CheckSummedBuffer{
	static final Logger logger = LoggerFactory.getLogger(CheckSummedBuffer.class);
	private final ByteBuffer buffer;
	private final BufferedDataOutputStreamPlus bufferStream;
	private final CRC32 checksum = new CRC32();
	public int blocks = 0;
	
	public CheckSummedBuffer(int bufferSize){
		buffer = ByteBuffer.allocateDirect(bufferSize);
		bufferStream = new DataOutputBufferFixed(buffer);
		blocks =  (int) (Math.ceil((double) bufferSize / (CapiBlockDevice.BLOCK_SIZE)));;
	}

	public void clear() {
		buffer.clear();		
	}
	
	public void clean(){
		 FileUtils.clean(buffer);
	}
	
	public BufferedDataOutputStreamPlus getStream() {
		return bufferStream;
	}
	
	public Checksum getChecksum(){
		return checksum;
	}
	
	public ByteBuffer  getBuffer(){
		return buffer;
	}
	
	public Checksum calculateCRC(int offset,int length){
		checksum.reset();
        int position = buffer.position();
        int limit = buffer.limit();
        buffer.position(offset).limit(offset + length);
        checksum.update(buffer);
        buffer.position(position).limit(limit);
		return checksum;
	}
}
