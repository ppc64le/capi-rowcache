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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.ibm.research.capiblock.Chunk;

class FlashBulkReplayer {
	private static int BULK_BLOCKS_TO_READ = 4000;// 32 MB pieces
	static final Logger logger = LoggerFactory.getLogger(FlashBulkReplayer.class);
	private static final int MAX_OUTSTANDING_REPLAY_COUNT = 2 * 1024 * 1024;
	private final Set<Keyspace> keyspacesRecovered;
	private final List<Future<?>> futures;
	private final Map<TableId, AtomicInteger> invalidMutations;
	private final AtomicInteger replayedCount;
	private final Map<TableId, CommitLogPosition> cfPositions;
	private final CommitLogPosition globalPosition;
	private final CRC32 checksum;
	private ByteBuffer buffer;
	private ByteBuffer readerBuffer;
	private long total_read = 0;
	private long total_deser = 0;

	FlashBulkReplayer() {
		this.keyspacesRecovered = new NonBlockingHashSet<Keyspace>();
		this.futures = new ArrayList<Future<?>>();
		buffer = ByteBuffer.allocate(CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS * 4096);
		this.invalidMutations = new HashMap<TableId, AtomicInteger>();
		this.replayedCount = new AtomicInteger();
		this.checksum = new CRC32();

		// compute per-CF and global replay positions
		cfPositions = new HashMap<TableId, CommitLogPosition>();
		Ordering<CommitLogPosition> replayPositionOrdering = Ordering.from(CommitLogPosition.comparator);
		for (ColumnFamilyStore cfs : ColumnFamilyStore.all()) {
			// it's important to call RP.gRP per-cf, before aggregating all the
			// positions w/ the Ordering.min call
			// below: gRP will return NONE if there are no flushed sstables,
			// which is important to have in the
			// list (otherwise we'll just start replay from the first flush
			// position that we do have, which is not correct).
			CommitLogPosition rp = CommitLogPosition.NONE;
			// but, if we've truncted the cf in question, then we need to need
			// to start replay after the truncation
			CommitLogPosition truncatedAt = SystemKeyspace.getTruncatedPosition(cfs.metadata.id);
			if (truncatedAt != null) {
				rp = replayPositionOrdering.max(Arrays.asList(rp, truncatedAt));
			}
			cfPositions.put(cfs.metadata.id, rp);
		}
		globalPosition = replayPositionOrdering.min(cfPositions.values());
		logger.debug("Global replay position is {} from columnfamilies {}" + globalPosition + "--- "
				+ FBUtilities.toString(cfPositions));

		// allocate reader blocks
		readerBuffer = ByteBuffer.allocateDirect((int) (BULK_BLOCKS_TO_READ * 1024 * 4));
	}

	void recover(FlashSegmentManager fsm) throws IOException {
		for (Integer key : fsm.unCommitted.keySet()) {
			buffer.clear();
			final long segmentId = fsm.unCommitted.get(key);
			int replayPosition;
			logger.debug("Global=" + globalPosition.segmentId);
			if (globalPosition.segmentId < segmentId) {
				replayPosition = 0;
			} else if (globalPosition.segmentId == segmentId) {
				replayPosition = globalPosition.position;
			} else {
				logger.debug("skipping replay of fully-flushed {}", key);
				continue;
			}
			logger.debug(segmentId + " Replaying " + key + " starting at " + replayPosition);
			// get the start position
			long claimedCRC32;
			int serializedSize;

			// read entire block starting from replay position
			Chunk ch = fsm.bookkeeper;
			logger.debug("ReplayPosition for key " + key + " reppos=" + replayPosition);
			long start = (CAPIFlashCommitLog.DATA_OFFSET + key * CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS) + replayPosition;
			long blocks = 0;
			long read_timer = System.currentTimeMillis();
			// TODO read 128 mb
			while (blocks != CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS) {
				readerBuffer.clear();
				logger.error("CAPI Reading " + start + " end:" + blocks + " realstart:"+(CAPIFlashCommitLog.DATA_OFFSET + key * CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS) + blocks +"realend:"+BULK_BLOCKS_TO_READ);
				ch.readBlock((CAPIFlashCommitLog.DATA_OFFSET + key * CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS) + blocks,
						BULK_BLOCKS_TO_READ, readerBuffer);
				blocks += BULK_BLOCKS_TO_READ;
				buffer.put(readerBuffer);
			}
			total_read += (System.currentTimeMillis() - read_timer);
			buffer.rewind();
			buffer.position(replayPosition);
			logger.debug(buffer.toString());
			long deser_timer = System.currentTimeMillis();
			while (buffer.remaining() != 0) {
				checksum.reset();
				int mark = buffer.position();
				long recordSegmentId = buffer.getLong();

				if (recordSegmentId != segmentId) {
					logger.debug("1st:" + recordSegmentId + "-- " + segmentId + "Unidentified segment!! at" + mark);
					break;
				}
				serializedSize = buffer.getInt();
				if (serializedSize < 38) {// 28 record bookeeping and checking
											// 10 minumum rm overhead
					logger.debug("Error!! Serialized Size is:" + serializedSize);
					break;
				}
				checksum.update(buffer.array(), mark, 12);
				buffer.position(mark + 12);

				long claimedSizeChecksum = buffer.getLong();
				if (checksum.getValue() != claimedSizeChecksum) {
					logger.debug("Error!! First Checksum Doesnot Match !! " + " Re ad:" + claimedSizeChecksum);
					break;
				}

				int blocksToRead = (int) (CAPIFlashCommitLog.getBlockCount(serializedSize));
				checksum.reset();
				buffer.position(buffer.position() + serializedSize - 28);
				claimedCRC32 = buffer.getLong();
				checksum.update(buffer.array(), mark + 20, serializedSize - 28);

				if (claimedCRC32 != checksum.getValue()) {
					logger.debug(
							"Error!! Second Checksum Doesnot Match !!" + claimedCRC32 + "   " + checksum.getValue());
					break;// TODO we check the record anyway, maybe continue
							// instead of break
				}

				buffer.position(mark + (blocksToRead * 4096));
				// now we are sure that our data is safe
				RebufferingInputStream bufIn = new DataInputBuffer(buffer.array(), mark + 20,
						serializedSize - 28);
				final Mutation rm;
				rm = Mutation.serializer.deserialize(bufIn, MessagingService.current_version,
						SerializationHelper.Flag.LOCAL);
				
				for (PartitionUpdate upd : rm.getPartitionUpdates())
					upd.validate();

				// check and compare with current replayposition
				final long entryLocation = buffer.position() / 4096;

				Runnable runnable = new WrappedRunnable() {
					@Override
					protected void runMayThrow() throws Exception {
						if (Schema.instance.getKeyspaceMetadata(rm.getKeyspaceName()) == null)
							return;
						final Keyspace keyspace = Keyspace.open(rm.getKeyspaceName());
						Mutation newRm = null;
						for (PartitionUpdate columnFamily : rm.getPartitionUpdates()) {
							//if (Schema.instance.getCF(columTableIdnFamily.id()) == null)
							//	continue; // dropped

							CommitLogPosition rp = cfPositions.get(columnFamily.metadata().id);
							if(rp==null){
								System.err.println("rp null");
								continue;
							}

							if (segmentId > rp.segmentId
									|| (segmentId == rp.segmentId && entryLocation > rp.position)) {
								if (newRm == null)
									newRm = new Mutation(rm.getKeyspaceName(), rm.key());
								newRm.add(columnFamily);
								replayedCount.incrementAndGet();
							}
						}
						if (newRm != null) {
							assert !newRm.isEmpty();
							Keyspace.open(newRm.getKeyspaceName()).apply(newRm, false);// donot
																						// write
																						// back
																						// to
																						// commitlog
							keyspacesRecovered.add(keyspace);
						}
					}
				};
				// logger.debug("Finished reading: " + key);
				futures.add(StageManager.getStage(Stage.MUTATION).submit(runnable));
				if (futures.size() > MAX_OUTSTANDING_REPLAY_COUNT) {
					FBUtilities.waitOnFutures(futures);
					futures.clear();
				}
			}
			total_deser += (System.currentTimeMillis() - deser_timer);
		}

	}

	int blockForWrites() {
		for (Map.Entry<TableId, AtomicInteger> entry : invalidMutations.entrySet()) {
			logger.debug(String.format("Skipped %d mutations from unknown (probably removed) CF with id %s",
					entry.getValue().intValue(), entry.getKey()));
		}
		// wait for all the writes to finish on the mutation stage
		FBUtilities.waitOnFutures(futures);
		logger.error("Deserialization:" + total_deser + " Reading:" + total_read);
		logger.error("Finished waiting on mutations from recovery");
		// flush replayed keyspaces
		futures.clear();
		for (Keyspace keyspace : keyspacesRecovered) {
			futures.addAll(keyspace.flush());
		}
		FBUtilities.waitOnFutures(futures);
		return replayedCount.get();
	}
}
