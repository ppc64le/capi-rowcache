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

import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.Chunk;

class SyncChunkManager extends ChunkManager {
	static final Logger logger = LoggerFactory.getLogger(SyncChunkManager.class);
	private final Semaphore semaphore = new Semaphore(CAPIFlashCommitLog.NUMBER_OF_ASYNC_WRITES, false);

	SyncChunkManager(int numAsync) {
		super(numAsync);
		logger.error("[SyncChunkManager - Devices =  " + CAPIFlashCommitLog.DEVICES.length + "," + numAsync + "]");
		logger.error("[SyncChunkManager - Concurrent Chunk Writers =  "
				+ CAPIFlashCommitLog.NUMBER_OF_ASYNC_WRITES + "Chunks:"
				+ NUMBER_OF_CHUNKS);
	}

	SyncChunkManager() {
		this(NUMBER_OF_ASYNC_CALLS_PER_CHUNK);
	}

        @Override
	void write(long startOffset, int num_blocks, CheckSummedBuffer buf) throws IOException {
		Chunk cur = getNextChunk();
		try {
			semaphore.acquireUninterruptibly();
			cur.writeBlock(startOffset, num_blocks, buf.getBuffer());
		} finally {
			semaphore.release();
		}
	}
}
