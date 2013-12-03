/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.bootstrap.FeedLifecycleListener;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    /**
     * Builds the job spec for ingesting a (primary) feed from its external source via the feed adaptor.
     * 
     * @param primaryFeed
     * @param metadataProvider
     * @return
     * @throws Exception
     */
    public static JobSpecification buildFeedIntakeJobSpec(PrimaryFeed primaryFeed, AqlMetadataProvider metadataProvider)
            throws Exception {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;

        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider.buildFeedIntakeRuntime(spec,
                    primaryFeed);
            feedIngestor = p.first;
            ingesterPc = p.second;
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return spec;

    }

    /**
     * Builds the job spec for sending message to an active feed to disconnect it from the
     * its source.
     * 
     * @param dataverseName
     * @param feedName
     * @param datasetName
     * @param metadataProvider
     * @param feedActivity
     * @return
     * @throws AsterixException
     * @throws AlgebricksException
     */
    public static JobSpecification buildDisconnectFeedJobSpec(String dataverseName, String feedName,
            String datasetName, AqlMetadataProvider metadataProvider, FeedConnectionId feedConnectionId)
            throws AsterixException, AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger;
        AlgebricksPartitionConstraint messengerPc;

        try {
            String[] locations = FeedLifecycleListener.INSTANCE.getIntakeLocations(feedConnectionId.getFeedId());
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider
                    .buildDisconnectFeedMessengerRuntime(spec, dataverseName, feedName, datasetName, locations);
            feedMessenger = p.first;
            messengerPc = p.second;
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return spec;

    }

}
