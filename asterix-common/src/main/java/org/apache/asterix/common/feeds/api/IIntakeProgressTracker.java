package org.apache.asterix.common.feeds.api;

import java.util.Map;

public interface IIntakeProgressTracker {

    public void configure(Map<String, String> configuration);

    public void notifyIngestedTupleTimestamp(long timestamp);

}
