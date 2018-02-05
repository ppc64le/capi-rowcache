package com.ibm.capiflash.cassandra.commitlog;

import java.io.IOException;
import java.util.zip.CRC32;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.AbstractCommitLogReader;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.net.MessagingService;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;

import com.ibm.research.capiblock.Chunk;

class CAPIFlashCommitLogReader extends AbstractCommitLogReader {

    private static int BULK_BLOCKS_TO_READ = 4000;// 32 MB pieces
    static final Logger logger = LoggerFactory.getLogger(CAPIFlashCommitLogReader.class);
    private final CRC32 checksum;
    private ByteBuffer buffer;
    private ByteBuffer readerBuffer;

    CAPIFlashCommitLogReader() {
        checksum = new CRC32();
        buffer = ByteBuffer.allocate(CAPIFlashCommitLog.SEGMENT_SIZE_IN_BLOCKS * 4096);
        readerBuffer = ByteBuffer.allocateDirect((int) (BULK_BLOCKS_TO_READ * 1024 * 4));
    }

    void readCommitLogSegment(CommitLogReadHandler handler, long segmentId, CommitLogPosition minPosition, FlashSegmentManager fsm) throws IOException {
        buffer.clear();
        final int key = fsm.getUnCommitted().get(segmentId);
        int replayPosition;
        logger.debug("Global=" + minPosition.segmentId);
        if (minPosition.segmentId < segmentId) {
            replayPosition = 0;
        } else if (minPosition.segmentId == segmentId) {
            replayPosition = minPosition.position;
        } else {
            logger.debug("skipping replay of fully-flushed {}", key);
            return;
        }
        logger.debug(segmentId + " Replaying " + key + " starting at " + replayPosition);
        // get the start position
        long claimedCRC32;
        int serializedSize;

        // read entire block starting from replay position
        Chunk ch = fsm.getBookkeeper();
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
                logger.debug("Error!! Second Checksum Doesnot Match !!" + claimedCRC32 + "   " + checksum.getValue());
                break;// TODO we check the record anyway, maybe continue
                // instead of break
            }
            buffer.position(mark + (blocksToRead * 4096));

            final CommitLogDescriptor desc = new CommitLogDescriptor(MessagingService.current_version, segmentId, null, new EncryptionContext());
            final int entryLocation = buffer.position() / 4096;
            readMutation(handler, buffer.array(), mark + 20, serializedSize - 28, minPosition, entryLocation, desc);
        }
    }
}
