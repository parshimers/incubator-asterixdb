package edu.uci.ics.asterix.common.feeds;

public interface IFeedWorkManager {

    public void submitWork(IFeedWork work, IFeedWorkEventListener listener);

}
