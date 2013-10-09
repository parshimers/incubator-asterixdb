package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;

public interface IFeedFrameWriter extends IFrameWriter {

    public FeedId getFeedId();
}
