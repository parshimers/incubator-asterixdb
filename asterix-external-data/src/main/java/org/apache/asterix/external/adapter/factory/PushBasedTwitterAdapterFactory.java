package org.apache.asterix.external.adapter.factory;

import java.util.Map;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.external.dataset.adapter.PushBasedTwitterAdapter;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class PushBasedTwitterAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private static final String NAME = "push_twitter";

    private ARecordType outputType;

    private Map<String, String> configuration;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        PushBasedTwitterAdapter twitterAdapter = new PushBasedTwitterAdapter(configuration, outputType, ctx);
        return twitterAdapter;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.outputType = outputType;
        this.configuration = configuration;
        TwitterUtil.initializeConfigurationWithAuthInfo(configuration);
        boolean requiredParamsSpecified = validateConfiguration(configuration);
        if (!requiredParamsSpecified) {
            StringBuilder builder = new StringBuilder();
            builder.append("One or more parameters are missing from adapter configuration\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_KEY + "\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_SECRET + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET + "\n");
            throw new Exception(builder.toString());
        }
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public boolean isRecordTrackingEnabled() {
        return false;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return null;
    }

    private boolean validateConfiguration(Map<String, String> configuration) {
        String consumerKey = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_KEY);
        String consumerSecret = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_SECRET);
        String accessToken = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN);
        String tokenSecret = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);

        if (consumerKey == null || consumerSecret == null || accessToken == null || tokenSecret == null) {
            return false;
        }
        return true;
    }

}
