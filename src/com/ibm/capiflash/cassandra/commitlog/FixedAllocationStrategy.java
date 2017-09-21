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

import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FixedAllocationStrategy implements BufferAllocationStrategy {
	private final Logger logger = LoggerFactory.getLogger(FixedAllocationStrategy.class);
	private final ConcurrentLinkedQueue<CheckSummedBuffer> buffers = new ConcurrentLinkedQueue<CheckSummedBuffer>();

	FixedAllocationStrategy() {
                final int NUMBER_OF_BUFFERS = (int)CAPIFlashCommitLog.parseOptionalNumberProperty("preallocated_buffer_count", "512");
                final int BUFFER_SIZE_IN_BLOCKS = (int)CAPIFlashCommitLog.parseOptionalNumberProperty("preallocated_buffer_size_in_blocks", "16");
		for (int i = 0; i < NUMBER_OF_BUFFERS; i++) {
			buffers.add(new CheckSummedBuffer(BUFFER_SIZE_IN_BLOCKS * 4096));
		}
	}

	@Override
	public CheckSummedBuffer poll(long requiredBlocks) {
		CheckSummedBuffer ret = null;
		// busy wait if resource is not available
		while ((ret = buffers.poll()) == null);
		return ret;
	}

	@Override
	public void free(CheckSummedBuffer buf) {
		buf.clear();
		buffers.add(buf);
	}

        @Override
        public void start() {
                // Do nothing
        }

        @Override
        public void stopUnsafe() {
                // Do nothing
        }
}
