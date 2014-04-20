package edu.uci.ics.asterix.common.feeds;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;

public class FeedCongestionMessage extends FeedMessage {

    private final FeedRuntimeId runtimeId;
    private int inflowRate;
    private int outflowRate;

    public FeedCongestionMessage(FeedRuntimeId runtimeId, int inflowRate, int outflowRate) {
        super(MessageType.CONGESTION);
        this.runtimeId = runtimeId;
        this.inflowRate = inflowRate;
        this.outflowRate = outflowRate;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(IFeedMessage.Constants.MESSAGE_TYPE, messageType.name());
        obj.put(IFeedMessage.Constants.DATAVERSE, runtimeId.getConnectionId().getFeedId().getDataverse());
        obj.put(IFeedMessage.Constants.FEED, runtimeId.getConnectionId().getFeedId().getFeedName());
        obj.put(IFeedMessage.Constants.DATASET, runtimeId.getConnectionId().getDatasetName());
        obj.put(IFeedMessage.Constants.RUNTIME_TYPE, runtimeId.getFeedRuntimeType());
        obj.put(IFeedMessage.Constants.PARTITION, runtimeId.getPartition());
        obj.put(IFeedMessage.Constants.INFLOW_RATE, inflowRate);
        obj.put(IFeedMessage.Constants.OUTFLOW_RATE, outflowRate);
        return obj;
    }

}
