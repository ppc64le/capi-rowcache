package com.ibm.capiflash.cassandra.commitlog;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.cassandra.db.commitlog.AbstractCommitLogReplayer;
import org.apache.cassandra.db.commitlog.AbstractCommitLogReader;

class CAPIFlashCommitLogReplayer extends AbstractCommitLogReplayer {

    private CAPIFlashCommitLogReader commitLogReader;

    CAPIFlashCommitLogReplayer(CAPIFlashCommitLog commitLog) {
	super(commitLog);
        commitLogReader = new CAPIFlashCommitLogReader();
    }

    @Override
    public AbstractCommitLogReader getCommitLogReader() {
        return commitLogReader;
    }

    void replayCAPIFlash(FlashSegmentManager fsm) throws IOException {
        List<Long> segmentIds = new ArrayList(fsm.getUnCommitted().keySet());
        Collections.sort(segmentIds);
        for (Long segmentId : segmentIds) {
            commitLogReader.readCommitLogSegment(this, segmentId, globalPosition, fsm);
        }
    }
}
