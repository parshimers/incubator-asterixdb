package edu.uci.ics.asterix.metadata.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.metadata.feeds.TimestampedADMDataParser;
import edu.uci.ics.asterix.metadata.feeds.TimestampedDelimitedDataParser;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AbstractTupleParser;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class AsterixTupleParserFactory implements IAsterixTupleParserFactory {

    private static final long serialVersionUID = 1L;

    public static enum InputDataFormat {
        ADM,
        DELIMITED,
        UNKNOWN
    }

    public static final String KEY_FORMAT = "format";
    public static final String FORMAT_ADM = "adm";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";

    public static final String BATCH_SIZE = "batch-size";
    public static final String BATCH_INTERVAL = "batch-interval";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PARSER_FACTORY = "parser";
    public static final String TIME_TRACKING = "time.tracking";
    public static final String AT_LEAST_ONE_SEMANTICS = FeedPolicyAccessor.AT_LEAST_ONE_SEMANTICS;

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

    private final ARecordType recordType;
    private final Map<String, String> configuration;
    private final InputDataFormat inputDataFormat;
    private int partition;

    public AsterixTupleParserFactory(Map<String, String> configuration, ARecordType recType, InputDataFormat dataFormat) {
        this.recordType = recType;
        this.configuration = configuration;
        this.inputDataFormat = dataFormat;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        ITupleParser tupleParser = null;
        try {
            String parserFactoryClassname = (String) configuration.get(KEY_PARSER_FACTORY);
            ITupleParserFactory parserFactory = null;
            if (parserFactoryClassname != null) {
                parserFactory = (ITupleParserFactory) Class.forName(parserFactoryClassname).newInstance();
                tupleParser = parserFactory.createTupleParser(ctx);
            } else {
                IDataParser dataParser = null;
                dataParser = createDataParser(ctx);
                tupleParser = new GenericTupleParser(ctx, recordType, dataParser);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        return tupleParser;
    }

    private static class GenericTupleParser extends AbstractTupleParser {

        private final IDataParser dataParser;

        public GenericTupleParser(IHyracksTaskContext ctx, ARecordType recType, IDataParser dataParser)
                throws HyracksDataException {
            super(ctx, recType);
            this.dataParser = dataParser;
        }

        @Override
        public IDataParser getDataParser() {
            return dataParser;
        }

    }

    private IDataParser createDataParser(IHyracksTaskContext ctx) throws Exception {
        IDataParser dataParser = null;
        switch (inputDataFormat) {
            case ADM:
                dataParser = configureADMParser(ctx);
                break;
            case DELIMITED:
                dataParser = configureDelimitedDataParser(ctx);
                break;
            case UNKNOWN:
                String specifiedFormat = (String) configuration.get(KEY_FORMAT);
                if (specifiedFormat == null) {
                    throw new IllegalArgumentException(" Unspecified data format");
                } else {
                    if (FORMAT_ADM.equalsIgnoreCase(specifiedFormat.toUpperCase())) {
                        dataParser = configureADMParser(ctx);
                    } else if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(specifiedFormat.toUpperCase())) {
                        dataParser = configureDelimitedDataParser(ctx);
                    } else {
                        throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT)
                                + " not supported");
                    }
                }
        }
        return dataParser;
    }

    private boolean validateTimeTrackingConstraint() {
        boolean timeTrackingEnabled = configuration.get(TIME_TRACKING) == null ? false : Boolean.valueOf(configuration
                .get(TIME_TRACKING));
        if (timeTrackingEnabled) {
            if (!recordType.isOpen()) {
                throw new IllegalArgumentException("Cannot enabled time tracking as open record type");
            }
            return true;
        }
        return false;
    }

    private IDataParser configureADMParser(IHyracksTaskContext ctx) {
        boolean injectTimestamps = validateTimeTrackingConstraint();
        boolean injectRecordId = configuration.get(AT_LEAST_ONE_SEMANTICS) == null ? false : Boolean
                .valueOf(configuration.get(AT_LEAST_ONE_SEMANTICS));
        if (injectRecordId || injectTimestamps) {
            return new TimestampedADMDataParser(ctx, partition, injectRecordId, injectTimestamps);
        } else {
            return new ADMDataParser();
        }
    }

    private IDataParser configureDelimitedDataParser(IHyracksTaskContext ctx) throws AsterixException {
        IValueParserFactory[] valueParserFactories = getValueParserFactories();
        Character delimiter = getDelimiter();
        return validateTimeTrackingConstraint() ? new TimestampedDelimitedDataParser(recordType, valueParserFactories,
                delimiter, partition) : new DelimitedDataParser(recordType, valueParserFactories, delimiter);
    }

    private static boolean isConditionalPushConfigured(Map<String, String> configuration) {
        String propValue = (String) configuration.get(BATCH_SIZE);
        int batchSize = propValue != null ? Integer.parseInt(propValue) : -1;
        propValue = (String) configuration.get(BATCH_INTERVAL);
        long batchInterval = propValue != null ? Long.parseLong(propValue) : -1;
        boolean conditionalPush = batchSize > 0 || batchInterval > 0;
        return conditionalPush;
    }

    private Character getDelimiter() throws AsterixException {
        String delimiterValue = (String) configuration.get(KEY_DELIMITER);
        if (delimiterValue != null && delimiterValue.length() > 1) {
            throw new AsterixException("improper delimiter");
        }
        Character delimiter = delimiterValue.charAt(0);
        return delimiter;
    }

    private IValueParserFactory[] getValueParserFactories() {
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
        return fieldParserFactories;
    }

    @Override
    public void initialize(int partition) {
        this.partition = partition;
    }

}
