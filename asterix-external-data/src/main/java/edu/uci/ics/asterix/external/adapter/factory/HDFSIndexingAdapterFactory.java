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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.HDFSIndexingAdapter;
import edu.uci.ics.asterix.external.indexing.dataflow.HDFSIndexingParserFactory;
import edu.uci.ics.asterix.external.indexing.dataflow.IndexingScheduler;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.InputSplitsFactory;

public class HDFSIndexingAdapterFactory extends HDFSAdapterFactory {

    private static final long serialVersionUID = 1L;

    private transient AlgebricksPartitionConstraint clusterLocations;
    private String[] readSchedule;
    private boolean executed[];
    private InputSplitsFactory inputSplitsFactory;
    private ConfFactory confFactory;
    private IAType atype;
    private boolean configured = false;
    public static IndexingScheduler hdfsScheduler;
    private static boolean initialized = false;
    private Map<String, String> configuration;

    public static final String HDFS_INDEXING_ADAPTER = "hdfs-indexing-adapter";

    private static IndexingScheduler initializeHDFSScheduler() {
        ICCContext ccContext = AsterixAppContextInfo.getInstance().getCCApplicationContext().getCCContext();
        IndexingScheduler scheduler = null;
        try {
            scheduler = new IndexingScheduler(ccContext.getClusterControllerInfo().getClientNetAddress(), ccContext
                    .getClusterControllerInfo().getClientNetPort());
        } catch (HyracksException e) {
            throw new IllegalStateException("Cannot obtain hdfs scheduler");
        }
        return scheduler;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return HDFS_INDEXING_ADAPTER;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (!configured) {
            throw new IllegalStateException("Adapter factory has not been configured yet");
        }
        return (AlgebricksPartitionConstraint) clusterLocations;
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        JobConf conf = confFactory.getConf();
        InputSplit[] inputSplits = inputSplitsFactory.getSplits();
        String nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
        ((HDFSIndexingParserFactory) parserFactory).setJobConf(conf);
        ((HDFSIndexingParserFactory) parserFactory).setArguments(configuration);
        HDFSIndexingAdapter hdfsIndexingAdapter = new HDFSIndexingAdapter(atype, readSchedule, executed, inputSplits,
                conf, clusterLocations, files, parserFactory, ctx, nodeName,
                (String) configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT),
                (String) configuration.get(AsterixTupleParserFactory.KEY_FORMAT));
        return hdfsIndexingAdapter;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        if (!initialized) {
            hdfsScheduler = initializeHDFSScheduler();
            initialized = true;
        }
        this.configuration = configuration;
        JobConf conf = HDFSAdapterFactory.configureJobConf(configuration);
        confFactory = new ConfFactory(conf);
        clusterLocations = getClusterLocations();
        InputSplit[] inputSplits = getSplits(conf);
        inputSplitsFactory = new InputSplitsFactory(inputSplits);
        readSchedule = hdfsScheduler.getLocationConstraints(inputSplits);
        executed = new boolean[readSchedule.length];
        Arrays.fill(executed, false);
        configured = true;
        atype = (IAType) outputType;
        // The function below is overwritten to create indexing adapter factory instead of regular adapter factory
        configureFormat(atype);
    }

    protected void configureFormat(IAType sourceDatatype) throws Exception {

        char delimiter = AsterixTupleParserFactory.getDelimiter(configuration);
        char quote = AsterixTupleParserFactory.getQuote(configuration, delimiter);

        parserFactory = new HDFSIndexingParserFactory((ARecordType) atype,
                configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT),
                configuration.get(AsterixTupleParserFactory.KEY_FORMAT), delimiter, quote,
                configuration.get(HDFSAdapterFactory.KEY_PARSER));
    }

    /**
     * A static function that creates and return delimited text data parser
     *
     * @param recordType
     *            (the record type to be parsed)
     * @param delimiter
     *            (the delimiter value)
     * @return
     */
    public static DelimitedDataParser getDelimitedDataParser(ARecordType recordType, char delimiter, char quote) {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                List<IAType> unionTypes = ((AUnionType) recordType.getFieldTypes()[i]).getUnionList();
                if (unionTypes.size() != 2 && unionTypes.get(0).getTypeTag() != ATypeTag.NULL) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionTypes.get(1).getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            IValueParserFactory vpf = valueParserFactoryMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;
        }
        return new DelimitedDataParser(recordType, fieldParserFactories, delimiter, quote, false);
    }

    public static AlgebricksPartitionConstraint getClusterLocations() {
        ArrayList<String> locs = new ArrayList<String>();
        Map<String, String[]> stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
        for (String i : stores.keySet()) {
            String[] nodeStores = stores.get(i);
            int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(i);
            for (int j = 0; j < nodeStores.length; j++) {
                for (int k = 0; k < numIODevices; k++) {
                    locs.add(i);
                }
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }

    private static Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = initializeValueParserFactoryMap();

    private static Map<ATypeTag, IValueParserFactory> initializeValueParserFactoryMap() {
        Map<ATypeTag, IValueParserFactory> m = new HashMap<ATypeTag, IValueParserFactory>();
        m.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        m.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        m.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        m.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        m.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        return m;
    }
}
