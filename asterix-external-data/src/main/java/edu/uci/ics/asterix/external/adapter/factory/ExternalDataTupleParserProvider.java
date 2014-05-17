package edu.uci.ics.asterix.external.adapter.factory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.metadata.feeds.ConditionalPushTupleParserFactory;
import edu.uci.ics.asterix.metadata.feeds.TimestampedADMDataParser;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.asterix.runtime.operators.file.GenericTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class ExternalDataTupleParserProvider {

    public static final String KEY_FORMAT = "format";
    public static final String BATCH_SIZE = "batch-size";
    public static final String BATCH_INTERVAL = "batch-interval";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PARSER_FACTORY = "parser";

    private static final Map<ATypeTag, IValueParserFactory> typeToValueParserFactMap = initTypeToValueParserMap();

    private static Map<ATypeTag, IValueParserFactory> initTypeToValueParserMap() {
        Map<ATypeTag, IValueParserFactory> map = new HashMap<ATypeTag, IValueParserFactory>();
        map.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        map.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        map.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        map.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        map.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        return map;
    }

    public enum Format {
        DELIMITED,
        ADM
    }

    public static ITupleParserFactory getTupleParserFactory(ARecordType outputType, Map<String, String> configuration,
            FeedPolicyAccessor policyAccessor) throws Exception {
        ITupleParserFactory parserFactory = null;
        String parserFactoryClassname = (String) configuration.get(KEY_PARSER_FACTORY);
        if (parserFactoryClassname == null) {
            String specifiedFormat = (String) configuration.get(KEY_FORMAT);
            if (specifiedFormat == null) {
                throw new IllegalArgumentException(" Unspecified data format");
            } else {
                IDataParser dataParser = null;
                switch (Format.valueOf(specifiedFormat)) {
                    case ADM:
                        if (policyAccessor.isTimeTrackingEnabled()) {
                            dataParser = new TimestampedADMDataParser();
                        } else {
                            dataParser = new ADMDataParser();
                        }
                        break;
                    case DELIMITED:
                        dataParser = getDelimitedDataParser(configuration, outputType);
                        break;
                    default:
                        throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT)
                                + " not supported");
                }

                if (isConditionalPushConfigured(configuration)) {
                    parserFactory = new ConditionalPushTupleParserFactory(outputType, configuration, policyAccessor,
                            dataParser);
                } else {
                    parserFactory = new GenericTupleParserFactory(dataParser, outputType);
                }
            }
        } else {
            parserFactory = (ITupleParserFactory) Class.forName(parserFactoryClassname).newInstance();
        }

        return parserFactory;
    }

    private static boolean isConditionalPushConfigured(Map<String, String> configuration) {
        String propValue = (String) configuration.get(BATCH_SIZE);
        int batchSize = propValue != null ? Integer.parseInt(propValue) : -1;
        propValue = (String) configuration.get(BATCH_INTERVAL);
        long batchInterval = propValue != null ? Long.parseLong(propValue) : -1;
        boolean conditionalPush = batchSize > 0 || batchInterval > 0;
        return conditionalPush;
    }

    private static Character getDelimiter(Map<String, String> configuration) throws AsterixException {
        String delimiterValue = (String) configuration.get(KEY_DELIMITER);
        if (delimiterValue != null && delimiterValue.length() > 1) {
            throw new AsterixException("improper delimiter");
        }

        Character delimiter = delimiterValue.charAt(0);
        return delimiter;
    }

    public static IDataParser getDelimitedDataParser(Map<String, String> configuration, ARecordType recordType)
            throws AsterixException {
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
            IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;
        }

        Character delimiter = getDelimiter(configuration);
        DelimitedDataParser dataParser = new DelimitedDataParser(recordType, fieldParserFactories, delimiter);
        return dataParser;
    }

}
