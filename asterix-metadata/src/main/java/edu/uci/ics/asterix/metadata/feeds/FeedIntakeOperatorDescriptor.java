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
package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedSubscriptionManager;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.functions.ExternalLibraryManager;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * Sends a control message to the registered message queue for feed specified by its feedId.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedIntakeOperatorDescriptor.class.getName());

    private final FeedId feedId;

    /** The adaptor factory that is used to create an instance of the feed adaptor **/
    private IAdapterFactory adapterFactory;

    /** The library that contains the adapter in use. **/
    private String adaptorLibraryName;

    /**
     * The adapter factory class that is used to create an instance of the feed adapter.
     * This value is used only in the case of external adapters.
     **/
    private String adapterFactoryClassName;

    /** The configuration parameters associated with the adapter. **/
    private Map<String, String> adapterConfiguration;

    private ARecordType adapterOutputType;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, PrimaryFeed primaryFeed, IAdapterFactory adapterFactory) {
        super(spec, 0, 1);
        this.feedId = new FeedId(primaryFeed.getDataverseName(), primaryFeed.getFeedName());
        this.adapterFactory = adapterFactory;
    }

    public FeedIntakeOperatorDescriptor(JobSpecification spec, PrimaryFeed primaryFeed, String adapterLibraryName,
            String adapterFactoryClassName, ARecordType adapterOutputType) {
        this(spec, primaryFeed, null);
        this.adapterFactoryClassName = adapterFactoryClassName;
        this.adaptorLibraryName = adapterLibraryName;
        this.adapterConfiguration = primaryFeed.getAdaptorConfiguration();
        this.adapterOutputType = adapterOutputType;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        IFeedSubscriptionManager feedSubscriptionManager = runtimeCtx.getFeedManager().getFeedSubscriptionManager();
        FeedSubscribableRuntimeId feedIngestionId = new FeedSubscribableRuntimeId(feedId, FeedRuntimeType.INTAKE,
                partition);
        IngestionRuntime ingestionRuntime = (IngestionRuntime) feedSubscriptionManager
                .getSubscribableRuntime(feedIngestionId);
        if (adapterFactory == null) {
            try {
                adapterFactory = createAdapterFactory(ctx, partition);
            } catch (Exception exception) {
                throw new HyracksDataException(exception);
            }
        }
        return new FeedIntakeOperatorNodePushable(ctx, feedId, adapterFactory, partition, ingestionRuntime);
    }

    public FeedId getFeedId() {
        return feedId;
    }

    private IAdapterFactory createAdapterFactory(IHyracksTaskContext ctx, int partition) throws Exception {
        ClassLoader classLoader = ExternalLibraryManager.getLibraryClassLoader(feedId.getDataverse(),
                adaptorLibraryName);
        IAdapterFactory adapterFactory = null;
        if (classLoader != null) {
            adapterFactory = ((IAdapterFactory) (classLoader.loadClass(adapterFactoryClassName).newInstance()));

            switch (adapterFactory.getAdapterType()) {
                case TYPED:
                    ((ITypedAdapterFactory) adapterFactory).configure(adapterConfiguration);
                    adapterOutputType = ((ITypedAdapterFactory) adapterFactory).getAdapterOutputType();
                    break;
                case GENERIC:
                    String outputTypeName = adapterConfiguration.get(IGenericAdapterFactory.KEY_TYPE_NAME);
                    if (outputTypeName == null) {
                        throw new IllegalArgumentException(
                                "You must specify the datatype associated with the incoming data. Datatype is specified by the "
                                        + IGenericAdapterFactory.KEY_TYPE_NAME + " configuration parameter");
                    }

                    ((IGenericAdapterFactory) adapterFactory).configure(adapterConfiguration,
                            (ARecordType) adapterOutputType);
                    break;
                default:
                    throw new IllegalStateException(" Unknown factory type for " + adapterFactoryClassName);
            }
        } else {
            String message = "Unable to create adapter as class loader not configured for library "
                    + adaptorLibraryName + " in dataverse " + feedId.getDataverse();
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(message);
            }
            throw new IllegalArgumentException(message);

        }
        return adapterFactory;
    }
}
