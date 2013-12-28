package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.IFeedMemoryComponent.Type;

public interface IFeedMemoryManager {

    public static final int START_COLLECTION_SIZE = 100;
    public static final int START_POOL_SIZE = 25;

    public IFeedMemoryComponent getMemoryComponent(Type type);

    public boolean expandMemoryComponent(IFeedMemoryComponent memoryComponent);

    public void releaseMemoryComponent(IFeedMemoryComponent memoryComponent);

}
