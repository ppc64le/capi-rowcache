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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.utils.IntegerInterval;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bsendir
 *
 */
class FlashSegment {
	static final Logger logger = LoggerFactory.getLogger(FlashSegment.class);
	
	 // a map of Cf->dirty interval in this segment; if interval is not covered by the clean set, the log contains unflushed data
        private final NonBlockingHashMap<TableId, IntegerInterval> cfDirty = new NonBlockingHashMap<>(1024);

        // a map of Cf->clean intervals; separate map from above to permit marking Cfs clean whilst the log is still in use
        private final ConcurrentHashMap<TableId, IntegerInterval.Set> cfClean = new ConcurrentHashMap<>();

	// create a unique id per segment
	private final static long idBase = System.currentTimeMillis();
	private final static AtomicInteger nextId = new AtomicInteger(1);

	// unique id of this segment
	final long id;
	// bookkeeping address
	final int physical_block_address;

	// total number of blocks in use for this segment 
	AtomicLong currentBlocks = new AtomicLong(0);

	static long getNextId() {
		return idBase + nextId.getAndIncrement();
	}

	FlashSegment(int pb_id) {
		id = getNextId();
		physical_block_address = pb_id;
	}

	boolean hasCapacityFor(long blocks) {
		return blocks <= (CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS- currentBlocks
				.get());
	}

	CommitLogPosition getContext() {
		return new CommitLogPosition(id, (int) currentBlocks.get());
	}

	long getID() {
		return id;
	}

	long getPB() {
		return physical_block_address;
	}

	
	long getandAddPosition(long blocks) {
		return (long) CAPIFlashCommitLog.DATA_OFFSET
				+ ((long) (physical_block_address) * CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS)
				+ (long) currentBlocks.getAndAdd(blocks);
	}

	static long calculatePos(long start, long segmentno) {
		return CAPIFlashCommitLog.DATA_OFFSET
				+ (long) ((segmentno) * CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS)
				+ start;
	}
	
        static<K> void coverInMap(ConcurrentMap<K, IntegerInterval> map, K key, int value) {
                IntegerInterval i = map.get(key);
                if (i == null)
                {
                        i = map.putIfAbsent(key, new IntegerInterval(value, value));
                        if (i == null)
                                // success
                                return;
                }
                i.expandToCover(value);
        }
    
	void markDirty(Mutation mutation, int allocatedPosition) {
		for (PartitionUpdate update : mutation.getPartitionUpdates())
            coverInMap(cfDirty, update.metadata().id, allocatedPosition);
	}
	

	synchronized void markClean(TableId cfId, CommitLogPosition startPosition, CommitLogPosition endPosition) {
                if (startPosition.segmentId > id || endPosition.segmentId < id)
                        return;
                if (!cfDirty.containsKey(cfId))
                        return;
        
                int start = startPosition.segmentId == id ? startPosition.position : 0;
                int end = endPosition.segmentId == id ? endPosition.position : Integer.MAX_VALUE;

                cfClean.computeIfAbsent(cfId, k -> new IntegerInterval.Set()).add(start, end);
        
                Iterator<Map.Entry<TableId, IntegerInterval.Set>> iter = cfClean.entrySet().iterator();
                while (iter.hasNext())
                {
                        Map.Entry<TableId, IntegerInterval.Set> clean = iter.next();
                        TableId xcfId = clean.getKey();
                        IntegerInterval.Set cleanSet = clean.getValue();
                        IntegerInterval dirtyInterval = cfDirty.get(xcfId);
                        if (dirtyInterval != null && cleanSet.covers(dirtyInterval))
                        {
                                cfDirty.remove(xcfId);
                                iter.remove();
                        }
                }
	}

	synchronized boolean isUnused() {
		  return cfDirty.isEmpty();
	}

	public boolean contains(CommitLogPosition context) {
		return context.segmentId == id;
	}
	
        /**
         * @return a collection of dirty CFIDs for this segment.
         */
        synchronized Collection<TableId> getDirtyCFIDs() {
                if (cfClean.isEmpty() || cfDirty.isEmpty())
                        return cfDirty.keySet();

                List<TableId> r = new ArrayList<>(cfDirty.size());
                for (Map.Entry<TableId, IntegerInterval> dirty : cfDirty.entrySet()) {
                        TableId cfId = dirty.getKey();
                        IntegerInterval dirtyInterval = dirty.getValue();
                        IntegerInterval.Set cleanSet = cfClean.get(cfId);
                        if (cleanSet == null || !cleanSet.covers(dirtyInterval))
                                r.add(dirty.getKey());
                }
                return r;
        }
	
	
	
	/**
	 * Used only for debugging
	 * @return
	 */
	String dirtyString() {
		StringBuilder sb = new StringBuilder();
		for (TableId cfId : getDirtyCFIDs()) {
			TableMetadata m = Schema.instance.getTableMetadata(cfId);
			sb.append(m == null ? "<deleted>" : m.name).append(" (")
					.append(cfId).append("), ");
		}
		return sb.toString();
	}
}
