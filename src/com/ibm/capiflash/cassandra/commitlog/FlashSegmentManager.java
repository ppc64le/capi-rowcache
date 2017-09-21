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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

/**
 * @author bsendir
 *
 */
class FlashSegmentManager {
	static final Logger logger = LoggerFactory.getLogger(FlashSegmentManager.class);

	static final ReentrantLock allocationLock = new ReentrantLock();

	final BlockingQueue<Integer> freelist = new LinkedBlockingQueue<Integer>(CAPIFlashCommitLog.NUMBER_OF_SEGMENTS);

	private final ConcurrentLinkedQueue<FlashSegment> activeSegments = new ConcurrentLinkedQueue<FlashSegment>();
	ByteBuffer util = ByteBuffer.allocateDirect(1024 * 4);// utility buffer for
															// bookkeping
															// purposes

	private final HashMap<Integer, Long> unCommitted;
        private final String bookkeeperDevice;
	private Chunk bookkeeper = null;
	private volatile FlashSegment active;

	FlashSegmentManager(String device) {
		bookkeeperDevice = device;
		unCommitted = new HashMap<Integer, Long>();
        }

        void start() throws IOException {
                bookkeeper = CapiBlockDevice.getInstance().openChunk(bookkeeperDevice);

                ByteBuffer recoverMe = ByteBuffer.allocateDirect(1024 * 4 * CAPIFlashCommitLog.NUMBER_OF_SEGMENTS);
                bookkeeper.readBlock(CAPIFlashCommitLog.START_OFFSET, CAPIFlashCommitLog.NUMBER_OF_SEGMENTS, recoverMe);
                for (int i = 0; i < CAPIFlashCommitLog.NUMBER_OF_SEGMENTS; i++) {
                        recoverMe.position(i * CapiBlockDevice.BLOCK_SIZE);
                        long segID = recoverMe.getLong();
                        if (segID != 0) {// Committed Segments will be 0 unCommitted
									// Segments will contain the unique id
                                logger.error(i + " is uncommitted with segment id " + segID);
                                unCommitted.put(i, segID);
                        }
                }

		for (int i = 0; i < CAPIFlashCommitLog.NUMBER_OF_SEGMENTS; i++) {
			if (!unCommitted.containsKey(i)) {
				freelist.add(i);
			} else {
				logger.error(i + " will be replayed");
			}
		}
		activateNextSegment();
	}

	private void activateNextSegment() throws IOException {
		Integer segid;
		try {
			segid = freelist.take();
			active = new FlashSegment(segid);

                        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 4);
                        logger.error("Activating " + active.getID() + " with PB:" + active.getPB() + " --> "
                                     + (CAPIFlashCommitLog.START_OFFSET + active.getPB()));
                        buf.putLong(active.getID());
                        bookkeeper.writeBlock(CAPIFlashCommitLog.START_OFFSET + active.getPB(), 1, buf);
			activeSegments.add(active);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	void recycleSegment(final FlashSegment segment) throws IOException {
		activeSegments.remove(segment);
                logger.error("Recycling " + segment.getID());
                util.putLong(0);
                bookkeeper.writeBlock(CAPIFlashCommitLog.START_OFFSET + segment.getPB(), 1, util);
                util.clear();
                freelist.add((int) segment.getPB());
	}

	Collection<FlashSegment> getActiveSegments() {
		return Collections.unmodifiableCollection(activeSegments);
	}

	FlashRecordAdder allocate(int num_blocks, Mutation rm) throws IOException {
		allocationLock.lock();
		if (freelist.isEmpty()) {
			allocationLock.unlock();
			return null;
		}
		if (active == null || !active.hasCapacityFor(num_blocks)) {
			activateNextSegment();
		}
		active.markDirty(rm, (int) active.currentBlocks.get());
		final FlashRecordAdder offset = new FlashRecordAdder(num_blocks, active.getandAddPosition(num_blocks),
				active.getID(), (int) active.currentBlocks.get());
		allocationLock.unlock();
		return offset;
	}

        CommitLogPosition getCurrentPosition() {
                return active.getContext();
        }

        HashMap<Integer, Long> getUnCommitted() {
                return unCommitted;
        }

        Chunk getBookkeeper() {
                return bookkeeper;
        }

	/**
	 * Zero all bookkeeping segments
	 */
	void recycleAfterReplay() throws IOException {
		for (Integer key : unCommitted.keySet()) {
                        util.putLong(0);
                        bookkeeper.writeBlock(CAPIFlashCommitLog.START_OFFSET + key, 1, util);
                        util.clear();
                        freelist.add(key);
                        logger.error("Recycle after replay activating: " + key);
		}
		unCommitted.clear();
	}

	private Future<?> flushDataFrom(List<FlashSegment> segments, boolean force) {
		if (segments.isEmpty())
			return Futures.immediateFuture(null);
		final CommitLogPosition maxReplayPosition = segments.get(segments.size() - 1).getContext();
		// a map of CfId -> forceFlush() to ensure we only queue one flush per
		// cf
		final Map<TableId, ListenableFuture<?>> flushes = new LinkedHashMap<>();
		for (FlashSegment segment : segments) {
			for (TableId dirtyCFId : segment.getDirtyCFIDs()) {
				TableMetadata metadata = Schema.instance.getTableMetadata(dirtyCFId);
				if (metadata == null) {
					// even though we remove the schema entry before a final
					// flush when dropping a CF,
					// it's still possible for a writer to race and finish his
					// append after the flush.
					logger.error("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
					segment.markClean(dirtyCFId, CommitLogPosition.NONE, segment.getContext());
				} else if (!flushes.containsKey(dirtyCFId)) {
					String keyspace = metadata.keyspace;
					final ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(dirtyCFId);
					// can safely call forceFlush here as we will only ever
					// block (briefly) for other attempts to flush,
					// no deadlock possibility since switchLock removal
					logger.error("Flushing " + dirtyCFId);
					flushes.put(dirtyCFId, force ? cfs.forceFlush() : cfs.forceFlush(maxReplayPosition));
				}
			}
		}

		return Futures.allAsList(flushes.values());
	}

	void forceRecycleAll(Iterable<TableId> droppedCfs) {
		// TODO
	}

        void shutdown() throws IOException {
                bookkeeper.close();
        }

        void stopUnsafe(boolean deleteSegments) throws IOException {
                freelist.clear();
                activeSegments.clear();
                unCommitted.clear();
                if (deleteSegments) {
                        for (int i = 0; i < CAPIFlashCommitLog.NUMBER_OF_SEGMENTS; i++) {
                                util.putLong(0);
                                bookkeeper.writeBlock(CAPIFlashCommitLog.START_OFFSET + i, 1, util);
                                util.clear();
                        }
                }
                bookkeeper.close();
                active = null;
        }
}
