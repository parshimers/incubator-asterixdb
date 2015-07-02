package org.apache.asterix.common.feeds.api;

public interface ITupleTrackingFeedAdapter extends IFeedAdapter {

    public void tuplePersistedTimeCallback(long timestamp);
}
