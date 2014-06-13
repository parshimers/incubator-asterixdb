package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class FeedFrameUtil {

    public static ByteBuffer getSlicedFrame(int frameSize, int tupleIndex, FrameTupleAccessor fta) {
        FrameTupleAppender appender = new FrameTupleAppender(frameSize);
        ByteBuffer slicedFrame = ByteBuffer.allocate(frameSize);
        appender.reset(slicedFrame, true);
        int startTupleIndex = tupleIndex + 1;
        int totalTuples = fta.getTupleCount();
        for (int ti = startTupleIndex; ti < totalTuples; ti++) {
            appender.append(fta, ti);
        }
        return slicedFrame;
    }

    public static ByteBuffer getSampledFrame(FrameTupleAccessor fta, int sampleSize, int frameSize) {
        NChooseKIterator it = new NChooseKIterator(fta.getTupleCount(), sampleSize);
        FrameTupleAppender appender = new FrameTupleAppender(frameSize);
        ByteBuffer sampledFrame = ByteBuffer.allocate(frameSize);
        appender.reset(sampledFrame, true);
        int nextTupleIndex = 0;
        while (it.hasNext()) {
            nextTupleIndex = it.next();
            appender.append(fta, nextTupleIndex);
        }
        return sampledFrame;
    }

    private static class NChooseKIterator {

        private final int n;
        private final int k;
        private final BitSet bSet;
        private final Random random;

        private int traversed = 0;

        public NChooseKIterator(int n, int k) {
            this.n = n;
            this.k = k;
            this.bSet = new BitSet(n);
            bSet.set(0, n - 1);
            this.random = new Random();
        }

        public boolean hasNext() {
            return traversed < k;
        }

        public int next() {
            traversed++;
            int startOffset = random.nextInt(n);
            int pos = -1;
            while (pos < 0) {
                pos = bSet.nextSetBit(startOffset);
                if (pos < 0) {
                    startOffset = (startOffset + 1) % n;
                }
            }
            bSet.clear(pos);
            return pos;
        }

    }

}
