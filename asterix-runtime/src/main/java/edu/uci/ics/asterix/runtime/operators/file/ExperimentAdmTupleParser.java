package edu.uci.ics.asterix.runtime.operators.file;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogManager;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ExperimentAdmTupleParser extends AdmTupleParser {

    private static final Logger LOGGER = Logger.getLogger(ExperimentAdmTupleParser.class.getName());

    private static final AtomicBoolean countMonitorActive = new AtomicBoolean();

    private final long duration;

    private final long pollFrequency;

    public ExperimentAdmTupleParser(IHyracksTaskContext ctx, ARecordType recType, long duration, long pollFrequency)
            throws HyracksDataException {
        super(ctx, recType);
        this.duration = duration;
        this.pollFrequency = pollFrequency;
    }

    @Override
    protected void postParserInitHook() {
        final long start = System.currentTimeMillis();
        IAsterixAppRuntimeContext rtCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
        final LogManager lm = (LogManager) rtCtx.getTransactionSubsystem().getLogManager();
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int count = lm.getCount();
                long end = System.currentTimeMillis();
                long totalSeconds = ((end - start) / 1000);
                double rps = count / totalSeconds;
                LOGGER.severe("duration = " + duration + ":\n\tIngested " + count + " records in " + totalSeconds
                        + " seconds [RPS = " + rps + "]");
            }
        });
        t.setName("Average Throughput Monitor");
        t.setDaemon(true);
        t.start();
        if (pollFrequency > 0) {
            synchronized (countMonitorActive) {
                if (!countMonitorActive.get()) {
                    Thread cmThread = new Thread(new Runnable() {

                        @Override
                        public void run() {
                            int count = lm.getCount();
                            while (true) {
                                try {
                                    Thread.sleep(pollFrequency * 1000);
                                    int newCount = lm.getCount();
                                    LOGGER.severe("Instantaneous throughput: " + (newCount - count) / pollFrequency);
                                    count = newCount;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    });
                    cmThread.setDaemon(true);
                    cmThread.setName("Instantaneous Throughput Monitor");
                    cmThread.start();
                    countMonitorActive.set(true);
                }
            }
        }
    }
}
