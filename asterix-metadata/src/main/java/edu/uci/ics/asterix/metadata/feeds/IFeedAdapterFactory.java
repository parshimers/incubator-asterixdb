package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;

public interface IFeedAdapterFactory extends IAdapterFactory {

    public boolean isRecordTrackingEnabled();

    public IIntakeProgressTracker createIntakeProgressTracker();

}
