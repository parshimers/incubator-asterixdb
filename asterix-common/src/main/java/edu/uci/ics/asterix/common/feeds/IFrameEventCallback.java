package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IFrameEventCallback {

    public enum FrameEvent {
        FINISHED_PROCESSING
    }

    public void frameEvent(FrameEvent frameEvent) throws HyracksDataException;
}
