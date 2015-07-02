package org.apache.asterix.external.dataset.adapter;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public interface IFeedClientFactory {

    public IPullBasedFeedClient createFeedClient(IHyracksTaskContext ctx, Map<String, String> configuration)
            throws Exception;

    public ARecordType getRecordType() throws AsterixException;

    public FeedClientType getFeedClientType();

    public enum FeedClientType {
        GENERIC,
        TYPED
    }
}
