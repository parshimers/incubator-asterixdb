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

import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.util.INodeResolver;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class StreamBasedAdapterFactory implements IAdapterFactory {

    private static final long serialVersionUID = 1L;
    protected static final Logger LOGGER = Logger.getLogger(StreamBasedAdapterFactory.class.getName());

    public static final String KEY_FORMAT = "format";
    public static final String KEY_PARSER_FACTORY = "parser";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PATH = "path";
    public static final String KEY_SOURCE_DATATYPE = "type-name";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_ADM = "adm";
    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";
    public static final String BATCH_SIZE = "batch-size";
    public static final String BATCH_INTERVAL = "batch-interval";

    protected Map<String, String> configuration;
    protected static INodeResolver nodeResolver;

    protected ITupleParserFactory parserFactory;

    protected void configureFormat(IAType sourceDatatype) throws Exception {
        FeedPolicyAccessor policyAccessor = null;
        parserFactory = ExternalDataTupleParserProvider.getTupleParserFactory(getAdapterOutputType(), configuration,
                policyAccessor);
    }

    public boolean isRecordTrackingEnabled() {
        return false;
    }

    public IIntakeProgressTracker createIntakeProgressTracker() {
        throw new UnsupportedOperationException("Tracking of ingested records not enabled");
    }

}
