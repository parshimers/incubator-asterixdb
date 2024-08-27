/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.exceptions;

import org.apache.asterix.common.utils.IdentifierUtil;
import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.util.ErrorMessageUtil;

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public enum ErrorCode implements IError {
    // Runtime errors
    CASTING_FIELD(1),
    TYPE_MISMATCH_FUNCTION(2),
    TYPE_INCOMPATIBLE(3),
    TYPE_UNSUPPORTED(4),
    TYPE_ITEM(5),
    INVALID_FORMAT(6),
    OVERFLOW(7),
    UNDERFLOW(8),
    INJECTED_FAILURE(9),
    NEGATIVE_VALUE(10),
    OUT_OF_BOUND(11),
    COERCION(12),
    DUPLICATE_FIELD_NAME(13),
    PROPERTY_NOT_SET(14),
    ROOT_LOCAL_RESOURCE_EXISTS(15),
    ROOT_LOCAL_RESOURCE_COULD_NOT_BE_CREATED(16),
    UNKNOWN_EXTERNAL_FILE_PENDING_OP(17),
    TYPE_CONVERT(18),
    TYPE_CONVERT_INTEGER_SOURCE(19),
    TYPE_CONVERT_INTEGER_TARGET(20),
    TYPE_CONVERT_OUT_OF_BOUND(21),
    FIELD_SHOULD_BE_TYPED(22),
    NC_REQUEST_TIMEOUT(23),
    POLYGON_INVALID_COORDINATE(24),
    POLYGON_3_POINTS(25),
    POLYGON_INVALID(26),
    OPERATION_NOT_SUPPORTED(27),
    INVALID_DURATION(28),
    UNKNOWN_DURATION_UNIT(29),
    REQUEST_TIMEOUT(30),
    INVALID_TYPE_CASTING_MATH_FUNCTION(31),
    REJECT_BAD_CLUSTER_STATE(32),
    REJECT_NODE_UNREGISTERED(33),
    UNSUPPORTED_MULTIPLE_STATEMENTS(35),
    CANNOT_COMPARE_COMPLEX(36),
    TYPE_MISMATCH_GENERIC(37),
    DIFFERENT_LIST_TYPE_ARGS(38),
    INTEGER_VALUE_EXPECTED(39),
    NO_STATEMENT_PROVIDED(40),
    REQUEST_CANCELLED(41),
    TPCDS_INVALID_TABLE_NAME(42),
    VALUE_OUT_OF_RANGE(43),
    PROHIBITED_STATEMENT_CATEGORY(44),
    INTEGER_VALUE_EXPECTED_FUNCTION(45),
    INVALID_LIKE_PATTERN(46),
    INVALID_REQ_PARAM_VAL(47),
    INVALID_REQ_JSON_VAL(48),
    PARAMETERS_REQUIRED(49),
    INVALID_PARAM(50),
    INCOMPARABLE_TYPES(51),
    ILLEGAL_STATE(52),
    UNSUPPORTED_PARQUET_TYPE(53),
    PARQUET_SUPPORTED_TYPE_WITH_OPTION(54),
    PARQUET_DECIMAL_TO_DOUBLE_PRECISION_LOSS(55),
    PARQUET_TIME_ZONE_ID_IS_NOT_SET(56),
    PARQUET_CONTAINS_OVERFLOWED_BIGINT(57),
    UNEXPECTED_ERROR_ENCOUNTERED(58),
    INVALID_PARQUET_FILE(59),
    FUNCTION_EVALUATION_FAILED(60),
    TYPE_MISMATCH_MISSING_FIELD(61),
    DIRECTORY_IS_NOT_EMPTY(62),
    COULD_NOT_CREATE_FILE(63),
    NON_STRING_WRITE_PATH(64),
    WRITE_PATH_LENGTH_EXCEEDS_MAX_LENGTH(65),
    TYPE_MISMATCH_EXTRA_FIELD(66),
    UNSUPPORTED_COLUMN_TYPE(67),
    INVALID_KEY_TYPE(68),
    FAILED_TO_READ_KEY(69),
    UNSUPPORTED_JRE(100),

    EXTERNAL_UDF_RESULT_TYPE_ERROR(200),
    EXTERNAL_UDF_EXCEPTION(201),
    EXTERNAL_UDF_PROTO_RETURN_EXCEPTION(202),

    // Compilation errors
    PARSE_ERROR(1001),
    COMPILATION_TYPE_MISMATCH_FUNCTION(1002),
    COMPILATION_TYPE_INCOMPATIBLE(1003),
    COMPILATION_TYPE_UNSUPPORTED(1004),
    COMPILATION_TYPE_ITEM(1005),
    COMPILATION_DUPLICATE_FIELD_NAME(1006),
    COMPILATION_INVALID_EXPRESSION(1007),
    COMPILATION_INVALID_PARAMETER_NUMBER(1008),
    COMPILATION_INVALID_RETURNING_EXPRESSION(1009),
    COMPILATION_FULLTEXT_PHRASE_FOUND(1010),
    COMPILATION_UNKNOWN_DATASET_TYPE(1011),
    COMPILATION_UNKNOWN_INDEX_TYPE(1012),
    COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD(1013),
    COMPILATION_FIELD_NOT_FOUND(1014),
    COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX(1015),
    COMPILATION_INDEX_TYPE_NOT_SUPPORTED_FOR_DATASET_TYPE(1016),
    COMPILATION_FILTER_CANNOT_BE_NULLABLE(1017),
    COMPILATION_ILLEGAL_FILTER_TYPE(1018),
    COMPILATION_CANNOT_AUTOGENERATE_COMPOSITE_KEY(1019),
    COMPILATION_ILLEGAL_AUTOGENERATED_TYPE(1020),
    COMPILATION_KEY_CANNOT_BE_NULLABLE(1021),
    COMPILATION_ILLEGAL_KEY_TYPE(1022),
    COMPILATION_CANT_DROP_ACTIVE_DATASET(1023),
    COMPILATION_FUNC_EXPRESSION_CANNOT_UTILIZE_INDEX(1026),
    COMPILATION_DATASET_TYPE_DOES_NOT_HAVE_PRIMARY_INDEX(1027),
    COMPILATION_UNSUPPORTED_QUERY_PARAMETER(1028),
    NO_METADATA_FOR_DATASET(1029),
    SUBTREE_HAS_NO_DATA_SOURCE(1030),
    SUBTREE_HAS_NO_ADDTIONAL_DATA_SOURCE(1031),
    NO_INDEX_FIELD_NAME_FOR_GIVEN_FUNC_EXPR(1032),
    NO_SUPPORTED_TYPE(1033),
    NO_TOKENIZER_FOR_TYPE(1034),
    INCOMPATIBLE_SEARCH_MODIFIER(1035),
    UNKNOWN_SEARCH_MODIFIER(1036),
    COMPILATION_BAD_QUERY_PARAMETER_VALUE(1037),
    COMPILATION_ILLEGAL_STATE(1038),
    COMPILATION_TWO_PHASE_LOCKING_VIOLATION(1039),
    DATASET_ID_EXHAUSTED(1040),
    INDEX_ILLEGAL_ENFORCED_NON_OPTIONAL(1041),
    INDEX_ILLEGAL_NON_ENFORCED_TYPED(1042),
    INDEX_RTREE_MULTIPLE_FIELDS_NOT_ALLOWED(1043),
    REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE(1044),
    ILLEGAL_LOCK_UPGRADE_OPERATION(1045),
    ILLEGAL_LOCK_DOWNGRADE_OPERATION(1046),
    UPGRADE_FAILED_LOCK_WAS_NOT_ACQUIRED(1047),
    DOWNGRADE_FAILED_LOCK_WAS_NOT_ACQUIRED(1048),
    LOCK_WAS_ACQUIRED_DIFFERENT_OPERATION(1049),
    UNKNOWN_DATASET_IN_DATAVERSE(1050),
    INDEX_ILLEGAL_ENFORCED_ON_CLOSED_FIELD(1051),
    INDEX_ILLEGAL_REPETITIVE_FIELD(1052),
    CANNOT_CREATE_SEC_PRIMARY_IDX_ON_EXT_DATASET(1053),
    COMPILATION_FAILED_DUE_TO_REPLICATE_OP(1054),
    COMPILATION_INCOMPATIBLE_FUNCTION_LANGUAGE(1055),
    TOO_MANY_OPTIONS_FOR_FUNCTION(1056),
    EXPRESSION_NOT_SUPPORTED_IN_CONSTANT_RECORD(1057),
    LITERAL_TYPE_NOT_SUPPORTED_IN_CONSTANT_RECORD(1058),
    UNSUPPORTED_WITH_FIELD(1059),
    WITH_FIELD_MUST_BE_OF_TYPE(1060),
    WITH_FIELD_MUST_CONTAIN_SUB_FIELD(1061),
    CONFIGURATION_PARAMETER_INVALID_TYPE(1062),
    UNKNOWN_DATAVERSE(1063),
    ERROR_OCCURRED_BETWEEN_TWO_TYPES_CONVERSION(1064),
    CHOSEN_INDEX_COUNT_SHOULD_BE_GREATER_THAN_ONE(1065),
    CANNOT_SERIALIZE_A_VALUE(1066),
    CANNOT_FIND_NON_MISSING_SELECT_OPERATOR(1067),
    CANNOT_GET_CONDITIONAL_SPLIT_KEY_VARIABLE(1068),
    CANNOT_DROP_INDEX(1069),
    METADATA_ERROR(1070),
    DATAVERSE_EXISTS(1071),
    DATASET_EXISTS(1072),
    UNDEFINED_IDENTIFIER(1073),
    AMBIGUOUS_IDENTIFIER(1074),
    FORBIDDEN_SCOPE(1075),
    NAME_RESOLVE_UNKNOWN_DATASET(1076),
    NAME_RESOLVE_UNKNOWN_DATASET_IN_DATAVERSE(1077),
    COMPILATION_UNEXPECTED_OPERATOR(1078),
    COMPILATION_ERROR(1079),
    UNKNOWN_NODEGROUP(1080),
    UNKNOWN_FUNCTION(1081),
    UNKNOWN_TYPE(1082),
    UNKNOWN_INDEX(1083),
    INDEX_EXISTS(1084),
    TYPE_EXISTS(1085),
    PARAMETER_NO_VALUE(1086),
    COMPILATION_INVALID_NUM_OF_ARGS(1087),
    FIELD_NOT_FOUND(1088),
    FIELD_NOT_OF_TYPE(1089),
    ARRAY_FIELD_ELEMENTS_MUST_BE_OF_TYPE(1090),
    COMPILATION_TYPE_MISMATCH_GENERIC(1091),
    ILLEGAL_SET_PARAMETER(1092),
    COMPILATION_TRANSLATION_ERROR(1093),
    RANGE_MAP_ERROR(1094),
    COMPILATION_EXPECTED_FUNCTION_CALL(1095),
    UNKNOWN_COMPRESSION_SCHEME(1096),
    UNSUPPORTED_WITH_SUBFIELD(1097),
    COMPILATION_INVALID_WINDOW_FRAME(1098),
    COMPILATION_UNEXPECTED_WINDOW_FRAME(1099),
    COMPILATION_UNEXPECTED_WINDOW_EXPRESSION(1100),
    COMPILATION_UNEXPECTED_WINDOW_ORDERBY(1101),
    COMPILATION_EXPECTED_WINDOW_FUNCTION(1102),
    COMPILATION_ILLEGAL_USE_OF_IDENTIFIER(1103),
    INVALID_FUNCTION_MODIFIER(1104),
    OPERATION_NOT_SUPPORTED_ON_PRIMARY_INDEX(1105),
    EXPECTED_CONSTANT_VALUE(1106),
    UNEXPECTED_HINT(1107),
    EXTERNAL_SOURCE_ERROR(1108),
    EXTERNAL_SOURCE_CONTAINER_NOT_FOUND(1109),
    PARAMETERS_NOT_ALLOWED_AT_SAME_TIME(1110),
    PROPERTY_INVALID_VALUE_TYPE(1111),
    INVALID_PROPERTY_FORMAT(1112),
    INVALID_REGEX_PATTERN(1113),
    EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES(1114),
    INVALID_DATABASE_OBJECT_NAME(1115),
    UNKNOWN_SYNONYM(1116),
    UNKNOWN_LIBRARY(1117),
    COMPILATION_GROUPING_SETS_OVERFLOW(1118),
    COMPILATION_GROUPING_OPERATION_INVALID_ARG(1119),
    COMPILATION_UNEXPECTED_ALIAS(1120),
    COMPILATION_ILLEGAL_USE_OF_FILTER_CLAUSE(1121),
    COMPILATION_BAD_FUNCTION_DEFINITION(1122),
    FUNCTION_EXISTS(1123),
    ADAPTER_EXISTS(1124),
    UNKNOWN_ADAPTER(1125),
    INVALID_EXTERNAL_IDENTIFIER_SIZE(1126),
    UNSUPPORTED_ADAPTER_LANGUAGE(1127),
    INCONSISTENT_FILTER_INDICATOR(1128),
    UNSUPPORTED_GBY_OBY_SELECT_COMBO(1129),
    ILLEGAL_RIGHT_OUTER_JOIN(1130),
    SYNONYM_EXISTS(1131),
    INVALID_HINT(1132),
    ONLY_SINGLE_AUTHENTICATION_IS_ALLOWED(1133),
    NO_AUTH_METHOD_PROVIDED(1134),
    NODE_EXISTS(1135),
    NODEGROUP_EXISTS(1136),
    COMPACTION_POLICY_EXISTS(1137),
    EXTERNAL_FILE_EXISTS(1138),
    FEED_EXISTS(1139),
    FEED_POLICY_EXISTS(1140),
    FEED_CONNECTION_EXISTS(1141),
    LIBRARY_EXISTS(1142),
    UNKNOWN_EXTERNAL_FILE(1143),
    UNKNOWN_FEED(1144),
    UNKNOWN_FEED_CONNECTION(1145),
    UNKNOWN_FEED_POLICY(1146),
    CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS(1147),
    CANNOT_DROP_OBJECT_DEPENDENT_EXISTS(1148),
    ILLEGAL_FUNCTION_OR_VIEW_RECURSION(1149),
    ILLEGAL_FUNCTION_USE(1150),
    NO_AUTH_PROVIDED_ENDPOINT_REQUIRED_FOR_ANONYMOUS_ACCESS(1151),
    FULL_TEXT_CONFIG_NOT_FOUND(1152),
    FULL_TEXT_FILTER_NOT_FOUND(1153),
    FULL_TEXT_DEFAULT_CONFIG_CANNOT_BE_DELETED_OR_CREATED(1154),
    COMPILATION_INCOMPATIBLE_INDEX_TYPE(1155),
    FULL_TEXT_CONFIG_ALREADY_EXISTS(1156),
    FULL_TEXT_FILTER_ALREADY_EXISTS(1157),
    COMPILATION_BAD_VIEW_DEFINITION(1158),
    UNKNOWN_VIEW(1159),
    VIEW_EXISTS(1160),
    UNSUPPORTED_TYPE_FOR_PARQUET(1161),
    INVALID_PRIMARY_KEY_DEFINITION(1162),
    UNSUPPORTED_AUTH_METHOD(1163),
    INVALID_FOREIGN_KEY_DEFINITION(1164),
    INVALID_FOREIGN_KEY_DEFINITION_REF_PK_NOT_FOUND(1165),
    INVALID_FOREIGN_KEY_DEFINITION_REF_PK_MISMATCH(1166),
    CANNOT_CHANGE_PRIMARY_KEY(1167),
    AMBIGUOUS_PROJECTION(1168),
    COMPILATION_SUBQUERY_COERCION_ERROR(1169),
    S3_REGION_NOT_SUPPORTED(1170),
    COMPILATION_SET_OPERATION_ERROR(1171),
    INVALID_TIMEZONE(1172),
    INVALID_PARAM_VALUE_ALLOWED_VALUE(1173),
    SAMPLE_HAS_ZERO_ROWS(1174),
    INVALID_SAMPLE_SIZE(1175),
    OUT_OF_RANGE_SAMPLE_SIZE(1176),
    INVALID_SAMPLE_SEED(1177),
    UNSUPPORTED_ICEBERG_TABLE(1178),
    UNSUPPORTED_ICEBERG_FORMAT_VERSION(1179),
    ERROR_READING_ICEBERG_METADATA(1180),
    UNSUPPORTED_COMPUTED_FIELD_TYPE(1181),
    FAILED_TO_CALCULATE_COMPUTED_FIELDS(1182),
    FAILED_TO_EVALUATE_COMPUTED_FIELD(1183),
    ILLEGAL_DML_OPERATION(1184),
    UNKNOWN_DATABASE(1185),
    DATABASE_EXISTS(1186),
    CANNOT_DROP_DATABASE_DEPENDENT_EXISTS(1187),
    UNSUPPORTED_WRITING_ADAPTER(1188),
    UNSUPPORTED_WRITING_FORMAT(1189),
    COMPUTED_FIELD_CONFLICTING_TYPE(1190),
    UNSUPPORTED_COLUMN_MERGE_POLICY(1191),
    UNSUPPORTED_COLUMN_LSM_FILTER(1192),
    UNKNOWN_STORAGE_FORMAT(1193),
    UNSUPPORTED_INDEX_IN_COLUMNAR_FORMAT(1194),
    COMPOSITE_KEY_NOT_SUPPORTED(1195),
    EXTERNAL_SINK_ERROR(1196),
    MINIMUM_VALUE_ALLOWED_FOR_PARAM(1197),
    DUPLICATE_FIELD_IN_PRIMARY_KEY(1198),
    INCOMPATIBLE_FIELDS_IN_PRIMARY_KEY(1199),
    PREFIX_SHOULD_NOT_START_WITH_SLASH(1200),
    INVALID_DELTA_TABLE_FORMAT(1201),
    UNSUPPORTED_WRITER_COMPRESSION_SCHEME(1202),
    INVALID_PARQUET_SCHEMA(1203),
    TYPE_UNSUPPORTED_PARQUET_WRITE(1204),
    INVALID_PARQUET_WRITER_VERSION(1205),
    ILLEGAL_SIZE_PROVIDED(1206),
    TYPE_UNSUPPORTED_CSV_WRITE(1207),
    INVALID_CSV_SCHEMA(1208),
    MAXIMUM_VALUE_ALLOWED_FOR_PARAM(1209),
    STORAGE_SIZE_NOT_APPLICABLE_TO_TYPE(1210),
    COULD_NOT_CREATE_TOKENS(1211),

    // Feed errors
    DATAFLOW_ILLEGAL_STATE(3001),
    UTIL_DATAFLOW_UTILS_TUPLE_TOO_LARGE(3002),
    UTIL_DATAFLOW_UTILS_UNKNOWN_FORWARD_POLICY(3003),
    OPERATORS_FEED_INTAKE_OPERATOR_DESCRIPTOR_CLASSLOADER_NOT_CONFIGURED(3004),
    PARSER_DELIMITED_NONOPTIONAL_NULL(3005),
    PARSER_DELIMITED_ILLEGAL_FIELD(3006),
    ADAPTER_TWITTER_TWITTER4J_LIB_NOT_FOUND(3007),
    OPERATORS_FEED_INTAKE_OPERATOR_NODE_PUSHABLE_FAIL_AT_INGESTION(3008),
    FEED_CREATE_FEED_DATATYPE_ERROR(3009),
    PARSER_HIVE_NON_PRIMITIVE_LIST_NOT_SUPPORT(3010),
    PARSER_HIVE_FIELD_TYPE(3011),
    PARSER_HIVE_GET_COLUMNS(3012),
    PARSER_HIVE_NO_CLOSED_COLUMNS(3013),
    PARSER_HIVE_NOT_SUPPORT_NON_OP_UNION(3014),
    PARSER_HIVE_MISSING_FIELD_TYPE_INFO(3015),
    PARSER_HIVE_NULL_FIELD(3016),
    PARSER_HIVE_NULL_VALUE_IN_LIST(3017),
    INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_NULL_IN_NON_OPTIONAL(3018),
    INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_CANNT_GET_PKEY(3019),
    FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED(3020),
    RECORD_READER_MALFORMED_INPUT_STREAM(3021),
    PROVIDER_DATAFLOW_CONTROLLER_UNKNOWN_DATA_SOURCE(3022),
    PROVIDER_DATASOURCE_FACTORY_UNKNOWN_INPUT_STREAM_FACTORY(3023),
    UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY(3024),
    UNKNOWN_RECORD_READER_FACTORY(3025),
    PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT(3026),
    UNKNOWN_RECORD_FORMAT_FOR_META_PARSER(3027),
    LIBRARY_JAVA_JOBJECTS_FIELD_ALREADY_DEFINED(3028),
    LIBRARY_JAVA_JOBJECTS_UNKNOWN_FIELD(3029),
    NODE_RESOLVER_NO_NODE_CONTROLLERS(3031),
    NODE_RESOLVER_UNABLE_RESOLVE_HOST(3032),
    INPUT_RECORD_CONVERTER_DCP_MSG_TO_RECORD_CONVERTER_UNKNOWN_DCP_REQUEST(3033),
    FEED_DATAFLOW_FRAME_DISTR_REGISTER_FAILED_DATA_PROVIDER(3034),
    INPUT_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE(3038),
    LIBRARY_JOBJECT_ACCESSOR_CANNOT_PARSE_TYPE(3039),
    LIBRARY_JOBJECT_UTIL_ILLEGAL_ARGU_TYPE(3040),
    LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS(3041),
    LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND(3042),
    LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND(3043),
    LIBRARY_EXTERNAL_LIBRARY_CLASS_REGISTERED(3044),
    LIBRARY_JAVA_FUNCTION_HELPER_CANNOT_HANDLE_ARGU_TYPE(3045),
    LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED(3046),
    LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_NAME(3047),
    OPERATORS_FEED_META_OPERATOR_DESCRIPTOR_INVALID_RUNTIME(3048),
    INVALID_DELIMITER(3049),
    INVALID_CHAR_LENGTH(3050),
    QUOTE_DELIMITER_MISMATCH(3051),
    INDEXING_EXTERNAL_FILE_INDEX_ACCESSOR_UNABLE_TO_FIND_FILE_INDEX(3052),
    PARSER_ADM_DATA_PARSER_FIELD_NOT_NULL(3053),
    PARSER_ADM_DATA_PARSER_TYPE_MISMATCH(3054),
    PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_KIND(3055),
    PARSER_ADM_DATA_PARSER_ILLEGAL_ESCAPE(3056),
    PARSER_ADM_DATA_PARSER_RECORD_END_UNEXPECTED(3057),
    PARSER_ADM_DATA_PARSER_EXTRA_FIELD_IN_CLOSED_RECORD(3058),
    PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_WHEN_EXPECT_COMMA(3059),
    PARSER_ADM_DATA_PARSER_FOUND_COMMA_WHEN(3060),
    PARSER_ADM_DATA_PARSER_UNSUPPORTED_INTERVAL_TYPE(3061),
    PARSER_ADM_DATA_PARSER_INTERVAL_NOT_CLOSED(3062),
    PARSER_ADM_DATA_PARSER_INTERVAL_BEGIN_END_POINT_MISMATCH(3063),
    PARSER_ADM_DATA_PARSER_INTERVAL_MISSING_COMMA(3064),
    PARSER_ADM_DATA_PARSER_INTERVAL_INVALID_DATETIME(3065),
    PARSER_ADM_DATA_PARSER_INTERVAL_UNSUPPORTED_TYPE(3066),
    PARSER_ADM_DATA_PARSER_INTERVAL_INTERVAL_ARGUMENT_ERROR(3067),
    PARSER_ADM_DATA_PARSER_LIST_FOUND_END_COLLECTION(3068),
    PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_BEFORE_LIST(3069),
    PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_EXPECTING_ITEM(3070),
    PARSER_ADM_DATA_PARSER_LIST_FOUND_END_RECOD(3071),
    PARSER_ADM_DATA_PARSER_CAST_ERROR(3072),
    PARSER_ADM_DATA_PARSER_CONSTRUCTOR_MISSING_DESERIALIZER(3073),
    PARSER_ADM_DATA_PARSER_WRONG_INSTANCE(3074),
    PARSER_EXT_DATA_PARSER_CLOSED_FIELD_NULL(3075),
    UTIL_FILE_SYSTEM_WATCHER_NO_FILES_FOUND(3076),
    UTIL_LOCAL_FILE_SYSTEM_UTILS_PATH_NOT_FOUND(3077),
    UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER(3078),
    ACTIVE_MANAGER_SHUTDOWN(3079),
    FEED_METADATA_UTIL_UNEXPECTED_FEED_DATATYPE(3080),
    FEED_METADATA_SOCKET_ADAPTOR_SOCKET_NOT_PROPERLY_CONFIGURED(3081),
    FEED_METADATA_SOCKET_ADAPTOR_SOCKET_INVALID_HOST_NC(3082),
    PROVIDER_DATASOURCE_FACTORY_DUPLICATE_FORMAT_MAPPING(3083),
    CANNOT_SUBSCRIBE_TO_FAILED_ACTIVE_ENTITY(3084),
    FEED_UNKNOWN_ADAPTER_NAME(3085),
    PROVIDER_STREAM_RECORD_READER_WRONG_CONFIGURATION(3086),
    FEED_CONNECT_FEED_APPLIED_INVALID_FUNCTION(3087),
    ACTIVE_MANAGER_INVALID_RUNTIME(3088),
    ACTIVE_ENTITY_ALREADY_STARTED(3089),
    ACTIVE_ENTITY_CANNOT_BE_STOPPED(3090),
    CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY(3091),
    CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY(3092),
    ACTIVE_ENTITY_IS_ALREADY_REGISTERED(3093),
    CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY(3094),
    CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY(3095),
    ACTIVE_NOTIFICATION_HANDLER_IS_SUSPENDED(3096),
    ACTIVE_ENTITY_LISTENER_IS_NOT_REGISTERED(3097),
    CANNOT_DERIGESTER_ACTIVE_ENTITY_LISTENER(3098),
    DOUBLE_INITIALIZATION_OF_ACTIVE_NOTIFICATION_HANDLER(3099),
    DOUBLE_RECOVERY_ATTEMPTS(3101),
    UNREPORTED_TASK_FAILURE_EXCEPTION(3102),
    ACTIVE_ENTITY_ALREADY_SUSPENDED(3103),
    ACTIVE_ENTITY_CANNOT_RESUME_FROM_STATE(3104),
    ACTIVE_RUNTIME_IS_ALREADY_REGISTERED(3105),
    ACTIVE_RUNTIME_IS_NOT_REGISTERED(3106),
    ACTIVE_EVENT_HANDLER_ALREADY_SUSPENDED(3107),
    FEED_FAILED_WHILE_GETTING_A_NEW_RECORD(3110),
    FEED_START_FEED_WITHOUT_CONNECTION(3111),
    PARSER_COLLECTION_ITEM_CANNOT_BE_NULL(3112),
    FAILED_TO_PARSE_RECORD(3113),
    FAILED_TO_PARSE_RECORD_CONTENT(3114),
    FAILED_TO_PARSE_METADATA(3115),
    INPUT_DECODE_FAILURE(3116),
    FAILED_TO_PARSE_MALFORMED_LOG_RECORD(3117),
    ACTIVE_ENTITY_NOT_RUNNING(3118),
    REQUIRED_PARAM_IF_PARAM_IS_PRESENT(3119),
    PARSER_DATA_PARSER_UNEXPECTED_TOKEN(3120),
    REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT(3121),
    PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT(3122),
    // Avro error
    UNSUPPORTED_TYPE_FOR_AVRO(3123),
    // Copy to CSV Error
    INVALID_QUOTE(3124),
    INVALID_FORCE_QUOTE(3125),
    INVALID_ESCAPE(3126),

    // Lifecycle management errors
    DUPLICATE_PARTITION_ID(4000),

    // Extension errors
    EXTENSION_ID_CONFLICT(4001),
    EXTENSION_COMPONENT_CONFLICT(4002),
    UNSUPPORTED_MESSAGE_TYPE(4003),
    INVALID_CONFIGURATION(4004),
    UNSUPPORTED_REPLICATION_STRATEGY(4005),

    // Lifecycle management errors pt.2
    CLUSTER_STATE_UNUSABLE(4006);

    private static final String RESOURCE_PATH = "asx_errormsg/en.properties";
    public static final String ASTERIX = "ASX";
    private final int intValue;

    ErrorCode(int intValue) {
        this.intValue = intValue;
    }

    @Override
    public String component() {
        return ASTERIX;
    }

    @Override
    public int intValue() {
        return intValue;
    }

    @Override
    public String errorMessage() {
        return ErrorMessageMapHolder.get(this);
    }

    private static class ErrorMessageMapHolder {
        private static final String[] enumMessages = IdentifierUtil
                .replaceIdentifiers(ErrorMessageUtil.defineMessageEnumOrdinalMap(values(), RESOURCE_PATH));

        private static String get(ErrorCode errorCode) {
            return enumMessages[errorCode.ordinal()];
        }
    }
}
