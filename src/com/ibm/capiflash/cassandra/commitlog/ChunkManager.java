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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

abstract class ChunkManager {
        static final int NUMBER_OF_CHUNKS = (int)CAPIFlashCommitLog.parseOptionalNumberProperty("number_of_chunks", "24");
        static final int NUMBER_OF_ASYNC_CALLS_PER_CHUNK = (int)CAPIFlashCommitLog.parseOptionalNumberProperty("async_calls_per_chunk", "128");
	static final Chunk chunks[] = new Chunk[NUMBER_OF_CHUNKS];
	final CapiBlockDevice dev = CapiBlockDevice.getInstance();
	final AtomicInteger nextChunk = new AtomicInteger(0);
        final int numAsync;

        ChunkManager(int numAsync) {
                this.numAsync = numAsync;
        }

	abstract void write(long l, int m, CheckSummedBuffer buf) throws IOException;

        void start() throws IOException {
                openChunks();
        }

        void stopUnsafe() throws IOException {
                closeChunks();
        }

        void shutdown() throws IOException {
                closeChunks();
        }

	private void openChunks() throws IOException {
		for (int i = 0; i < chunks.length; i++) {
                        if (numAsync == 0) {
                                // let the device decide max num of requests
                                chunks[i] = dev.openChunk(CAPIFlashCommitLog.DEVICES[i % CAPIFlashCommitLog.DEVICES.length]);
                        } else {
                                // user defined max requests per chunk
                                chunks[i] = dev.openChunk(CAPIFlashCommitLog.DEVICES[i % CAPIFlashCommitLog.DEVICES.length], numAsync);
                        }
		}
	}

	private void closeChunks() throws IOException {
		for (int i = 0; i < chunks.length; i++) {
                        chunks[i].close();
		}
	}

	protected Chunk getNextChunk() {
		return chunks[Math.abs(nextChunk.getAndIncrement() % chunks.length)];
	}
}
