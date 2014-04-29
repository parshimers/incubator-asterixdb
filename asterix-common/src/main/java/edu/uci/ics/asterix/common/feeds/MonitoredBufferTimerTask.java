package edu.uci.ics.asterix.common.feeds;

import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback.FrameEvent;

public class MonitoredBufferTimerTask extends TimerTask {

    private static final Logger LOGGER = Logger.getLogger(MonitoredBufferTimerTask.class.getName());

    private static final int PENDING_WORK_THRESHOLD = 10;

    private static final int MAX_SUCCESSIVE_THRESHOLD_PERIODS = 2;

    private final MonitoredBuffer mBuffer;
    private int pendingWorkExceedCount = 0;
    private final IFrameEventCallback callback;
    private FrameEvent lastEvent = FrameEvent.NO_OP;

    public MonitoredBufferTimerTask(MonitoredBuffer mBuffer, IFrameEventCallback callback) {
        this.mBuffer = mBuffer;
        this.callback = callback;
    }

    @Override
    public void run() {
        /*
        if (LOGGER.isLoggable(Level.INFO)) {
           LOGGER.info(mBuffer.getRuntimeId() + " " + "Outflow rate:" + mBuffer.getOutflowRate() + " Inflow Rate:"
                   + mBuffer.getInflowRate());
        }*/
        int pendingWork = mBuffer.getWorkSize();
        if (mBuffer.getMode().equals(Mode.PROCESS_SPILL) || mBuffer.getMode().equals(Mode.PROCESS_BACKLOG)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Not acting while spillage processing in progress " + pendingWork);
            }
            return;
        }

        /*
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Pending work for " + mBuffer.getRuntimeId() + " " + pendingWork);
        }*/

        switch (lastEvent) {
            case NO_OP:
            case PENDING_WORK_DONE:
            case FINISHED_PROCESSING_SPILLAGE:
                if (pendingWork > PENDING_WORK_THRESHOLD) {
                    pendingWorkExceedCount++;
                    if (pendingWorkExceedCount > MAX_SUCCESSIVE_THRESHOLD_PERIODS) {
                        pendingWorkExceedCount = 0;
                        lastEvent = FrameEvent.PENDING_WORK_THRESHOLD_REACHED;
                        callback.frameEvent(lastEvent);
                    }
                }
                break;
            case PENDING_WORK_THRESHOLD_REACHED:
                if (pendingWork == 0) {
                    lastEvent = FrameEvent.PENDING_WORK_DONE;
                    callback.frameEvent(lastEvent);
                }
                break;
            case FINISHED_PROCESSING:
                break;

        }
    }
}
