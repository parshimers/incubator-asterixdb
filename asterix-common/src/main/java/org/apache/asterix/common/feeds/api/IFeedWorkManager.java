package org.apache.asterix.common.feeds.api;

public interface IFeedWorkManager {

    public void submitWork(IFeedWork work, IFeedWorkEventListener listener);

}
