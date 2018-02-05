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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.ICommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;

class CAPIFlashCommitLog implements ICommitLog {
	static final Logger logger;
        static final String PROPERTY_PREFIX;
	static final String[] DEVICES;
	static final long START_OFFSET;
        static final int NUMBER_OF_SEGMENTS;
	static final long DATA_OFFSET;
        static final int SEGMENT_SIZE_IN_BLOCKS;
        static final int NUMBER_OF_ASYNC_WRITES;
	static final CAPIFlashCommitLog instance;
	private FlashSegmentManager fsm;
	private ChunkManager chunkManager;
	private BufferAllocationStrategy bufferAlloc;

        static {
                logger = LoggerFactory.getLogger(CAPIFlashCommitLog.class);

                PROPERTY_PREFIX = "com.ibm.capiflash.cassandra.commitlog.";
                final String PROP_devices = "devices";
                final String PROP_start_offset = "start_offset";
                final String PROP_number_of_segments = "number_of_segments";
                final String PROP_segment_size_in_blocks = "segment_size_in_blocks";
                final String PROP_number_of_concurrent_writeBlock = "number_of_concurrent_writeBlock";

                final String STR_devices = System.getProperty(PROPERTY_PREFIX + PROP_devices);
                if (STR_devices == null) {
                        logger.error("Necessary property " + PROPERTY_PREFIX + PROP_devices + " not specified");
                        throw new IllegalStateException("Necessary property " + PROPERTY_PREFIX + PROP_devices + " not specified");
                }
                DEVICES = STR_devices.split(",");

                START_OFFSET = parseRequiredNumberProperty(PROP_start_offset);
                NUMBER_OF_SEGMENTS = (int)parseRequiredNumberProperty(PROP_number_of_segments);
                DATA_OFFSET = START_OFFSET + NUMBER_OF_SEGMENTS;
                SEGMENT_SIZE_IN_BLOCKS = (int)parseRequiredNumberProperty(PROP_segment_size_in_blocks);

                NUMBER_OF_ASYNC_WRITES = (int)parseOptionalNumberProperty(PROP_number_of_concurrent_writeBlock, "128");

                try {
                        instance = new CAPIFlashCommitLog();
                        instance.start();
                } catch (IOException e) {
                        throw new CAPIFlashError(e, DEVICES[0]);
                }
        }

        static long parseRequiredNumberProperty(final String property) {
                final String prefixedProperty = PROPERTY_PREFIX + property;
                final String specifiedString = System.getProperty(prefixedProperty);
                if (specifiedString == null) {
                        logger.error("Necessary property " + prefixedProperty + " not specified");
                        throw new IllegalStateException("Necessary property " + prefixedProperty + " not specified");
                }
                final long value;
                try {
                        value = Long.parseLong(specifiedString);
                } catch (NumberFormatException nfe) {
                        logger.error(specifiedString + " specified by " + prefixedProperty + " is not a valid number");
                        throw new IllegalStateException(specifiedString + " specified by " + prefixedProperty + " is not a valid number");
                }
                return value;
        }

        static long parseOptionalNumberProperty(final String property, final String defaultValueString) {
                final String prefixedProperty = PROPERTY_PREFIX + property;
                final String specifiedString = System.getProperty(prefixedProperty, defaultValueString);
                final long value;
                try {
                        value = Long.parseLong(specifiedString);
                } catch (NumberFormatException nfe) {
                        logger.error(specifiedString + " specified by " + prefixedProperty + " is not a valid number");
                        throw new IllegalStateException(specifiedString + " specified by " + prefixedProperty + " is not a valid number");
                }
                return value;
        }

	protected CAPIFlashCommitLog() throws IOException {
                logger.info("Setting up CapiFlash Commitlog");
                fsm = new FlashSegmentManager(DEVICES[0]);

                final String PROP_chunkmanager_type = PROPERTY_PREFIX + "chunkmanager_type";
                final String STR_chunkmanager_type = System.getProperty(PROP_chunkmanager_type, "SyncChunkManager");
                if (STR_chunkmanager_type.equals("SyncChunkManager")) {
                        chunkManager = new SyncChunkManager();
                } else if (STR_chunkmanager_type.equals("AsyncChunkManager")) {
                        chunkManager = new AsyncChunkManager();
                } else {
                        logger.error(STR_chunkmanager_type + " specified by " + PROP_chunkmanager_type + " is unknown");
                        throw new IllegalStateException(STR_chunkmanager_type + " specified by " + PROP_chunkmanager_type + " is unknown");
                }

                final String PROP_buffer_allocator_type = PROPERTY_PREFIX + "buffer_allocator_type";
                final String STR_buffer_allocator_type = System.getProperty(PROP_buffer_allocator_type, "FixedSizeAllocationStrategy");
                if (STR_buffer_allocator_type.equals("FixedSizeAllocationStrategy")) {
                        bufferAlloc = new FixedAllocationStrategy();
                } else if (STR_buffer_allocator_type.equals("PooledAllocationStrategy")) {
                        bufferAlloc = new PooledAllocationStrategy(NUMBER_OF_ASYNC_WRITES);
                } else {
                        logger.error(STR_buffer_allocator_type + " specified by " + PROP_buffer_allocator_type + " is unknown");
                        throw new IllegalStateException(STR_buffer_allocator_type + " specified by " + PROP_buffer_allocator_type + " is unknown");
                }
	}

        private CAPIFlashCommitLog start() throws IOException {
                fsm.start();
                chunkManager.start();
                bufferAlloc.start();
                return this;
        }

	/**
	 * Appends row mutation to CommitLog
	 * 
	 * @param
	 */
	@Override
	public CommitLogPosition add(Mutation rm) {
		assert rm != null;
		long totalSize = Mutation.serializer.serializedSize(rm, MessagingService.current_version) + 28;
		int requiredBlocks = getBlockCount(totalSize);
		if (requiredBlocks > SEGMENT_SIZE_IN_BLOCKS) {
			throw new IllegalArgumentException(
					String.format("Mutation of %s blocks is too large for the maxiumum size of %s", totalSize,
                                                      SEGMENT_SIZE_IN_BLOCKS));
		}
		FlashRecordAdder adder = null;
                try {
                        adder = fsm.allocate(requiredBlocks, rm);
                } catch (IOException e) {
                        throw new CAPIFlashError(e, DEVICES[0]);
                }
		//if we are out of space we immediately return to callee and throw exception
		//callee releases oporder for the current thread when exception occurs
		if (adder == null) {
			return null;
		}
		CheckSummedBuffer buf = null;
		buf = bufferAlloc.poll(requiredBlocks);
		try {
			buf.getBuffer().putLong(adder.getSegmentID());
			buf.getBuffer().putInt((int) totalSize);
			buf.getBuffer().putLong(buf.calculateCRC(0, 12).getValue()); 
			Mutation.serializer.serialize(rm, buf.getStream(), MessagingService.current_version);
			buf.getBuffer().putLong(buf.calculateCRC(20, ((int) totalSize) - 28).getValue()); 
                        chunkManager.write(adder.getStartBlock(), adder.getRequiredBlocks(), buf);
		} catch (IOException e) {
                        throw new CAPIFlashError(e, DEVICES[0]);
		} finally {
                        bufferAlloc.free(buf);
                }
		return new CommitLogPosition(adder.getSegmentID(), adder.getOffset());
	}

	/**
	 * Modifies the per-CF dirty cursors of any commit log segments for the
	 * column family according to the position given. Recycles it if it is
	 * unused. Called from org.apache.cassandra.db.ColumnFamilyStore.java at the
	 * end of flush operation
	 * 
	 * @param cfId
	 *            the column family ID that was flushed
	 * @param context
	 *            the replay position of the flush
	 */
	@Override
	public void discardCompletedSegments(TableId cfId, final CommitLogPosition lowerBound, CommitLogPosition upperBound) {
		// Go thru the active segment files, which are ordered oldest to
		// newest, marking the
		// flushed CF as clean, until we reach the segment file
		// containing the ReplayPosition passed
		// in the arguments. Any segments that become unused after they
		// are marked clean will be
		// recycled or discarded
		 logger.error("discard completed log segments for {}-{}, table {}", lowerBound, upperBound, cfId);
		for (Iterator<FlashSegment> iter = fsm.getActiveSegments().iterator(); iter.hasNext();) {
			FlashSegment segment = iter.next();
			segment.markClean(cfId, lowerBound, upperBound);
			// If the segment is no longer needed, and we have another
			// spare segment in the hopper
			// (to keep the last segment from getting discarded), pursue
			// either recycling or deleting
			// this segment file.
			if (iter.hasNext()) {
				if (segment.isUnused()) {
					logger.error("Commit log segment {} is unused ", segment.physical_block_address);
                                        try {
                                                fsm.recycleSegment(segment);
                                        } catch (IOException e) {
                                                throw new CAPIFlashError(e, DEVICES[0]);
                                        }
				} else {
					logger.error("Not safe to delete commit log segment {}; dirty is {} ",
							segment.physical_block_address, segment.dirtyString());
				}
			} else {
				logger.error("Not deleting active commitlog segment {} ", segment.physical_block_address);
			}
                        if (segment.contains(upperBound)) {
                                logger.error("Segment " + segment.id + " contains the context");
                                break;
                        }
		}
	}

	/**
	 * Recover
	 */
	@Override
	public int recover() throws IOException {
		long startTime = System.currentTimeMillis();
		CAPIFlashCommitLogReplayer r = new CAPIFlashCommitLogReplayer(this);
                r.replayCAPIFlash(fsm);
		long count = r.blockForWrites();
		fsm.recycleAfterReplay();
		long estimatedTime = System.currentTimeMillis() - startTime;
		logger.error("Replayed " + count + " records in " + estimatedTime + " ms");
		return (int) count;
	}

	/**
	 * Shuts down the threads used by the commit log, blocking until completion.
	 */
	@Override
	public void shutdownBlocking() {
		try {
                        chunkManager.shutdown();
                        fsm.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**/
	@Override
	public CommitLogPosition getCurrentPosition() {
		return fsm.getCurrentPosition();
	}

	static int getBlockCount(long size) {
		return (int) (Math.ceil((double) size / (CapiBlockDevice.BLOCK_SIZE)));
	}

	@Override
	public void forceRecycleAllSegments() {
		forceRecycleAllSegments(Collections.<TableId> emptyList());
	}

	@Override
	public void forceRecycleAllSegments(Iterable<TableId> droppedCfs) {
		fsm.forceRecycleAll(droppedCfs);
	}

        @Override
        public CommitLogArchiver getArchiver() {
                return null;
        }

        /**
         * FOR TESTING PURPOSES
         * @return the number of files recovered
         */
        @Override
        public int resetUnsafe(boolean deleteSegments) throws IOException {
                stopUnsafe(deleteSegments);
                return restartUnsafe();
        }

        /**
         * FOR TESTING PURPOSES
         */
        @Override
        public void stopUnsafe(boolean deleteSegments) throws IOException {
                fsm.stopUnsafe(deleteSegments);
                chunkManager.stopUnsafe();
                bufferAlloc.stopUnsafe();
        }

        /**
         * FOR TESTING PURPOSES
         */
        @Override
        public int restartUnsafe() throws IOException {
                return start().recover();
        }
}
