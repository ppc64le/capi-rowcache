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

/**
 * @author bsendir Utility Class to keep offsets for a record.
 *
 */

class FlashRecordAdder {
	private long startBlock = 0;
	private int requiredBlocks = 0;
	private long segmentID = 0;
	private int offset = 0;

	FlashRecordAdder(int num_blocks, long pos, long id, int n_offset) {
		requiredBlocks = num_blocks;
		segmentID = id;
		startBlock = pos;
		offset = n_offset;
	}

	long getStartBlock() {
		return startBlock;
	}

	int getRequiredBlocks() {
		return requiredBlocks;
	}

	long getSegmentID() {
		return segmentID;
	}
	
	int getOffset(){
		return offset;
	}
}
