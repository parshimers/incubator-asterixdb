package org.apache.asterix.external.dataset.adapter;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.parse.ITupleForwardPolicy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class PushBasedTwitterAdapter extends ClientBasedFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 50;

    private PushBasedTwitterFeedClient tweetClient;

    public PushBasedTwitterAdapter(Map<String, String> configuration, ARecordType recordType, IHyracksTaskContext ctx) throws AsterixException {
        super(configuration, ctx);
        this.configuration = configuration;
        this.tweetClient = new PushBasedTwitterFeedClient(ctx, recordType, this);
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        return true;
    }

    @Override
    public IFeedClient getFeedClient(int partition) throws Exception {
        return tweetClient;
    }

    @Override
    public ITupleForwardPolicy getTupleParserPolicy() {
        configuration.put(ITupleForwardPolicy.PARSER_POLICY,
                ITupleForwardPolicy.TupleForwardPolicyType.COUNTER_TIMER_EXPIRED.name());
        String propValue = configuration.get(CounterTimerTupleForwardPolicy.BATCH_SIZE);
        if (propValue == null) {
            configuration.put(CounterTimerTupleForwardPolicy.BATCH_SIZE, "" + DEFAULT_BATCH_SIZE);
        }
        return AsterixTupleParserFactory.getTupleParserPolicy(configuration);
    }

}
