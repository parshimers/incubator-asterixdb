package edu.uci.ics.asterix.common.feeds;

public interface IFeedSubscriptionManager {

    /**
     * @param subscribableRuntime
     */
    public void registerFeedSubscribableRuntime(ISubscribableRuntime subscribableRuntime);

    /**
     * @param subscribableRuntimeId
     */
    public void deregisterFeedSubscribableRuntime(FeedSubscribableRuntimeId subscribableRuntimeId);

    /**
     * @param subscribableRuntimeId
     * @return
     */
    public ISubscribableRuntime getSubscribableRuntime(FeedSubscribableRuntimeId subscribableRuntimeId);
}
