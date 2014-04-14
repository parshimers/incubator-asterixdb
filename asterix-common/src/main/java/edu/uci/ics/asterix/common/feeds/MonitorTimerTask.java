package edu.uci.ics.asterix.common.feeds;

import java.util.TimerTask;

public class MonitorTimerTask extends TimerTask {

    private final MonitoredBuffer mBuffer;
    
    public MonitorTimerTask(MonitoredBuffer mBuffer) {
        this.mBuffer = mBuffer;
    }

    @Override
    public void run() {
        mBuffer.getWorkSize();
    }

}
