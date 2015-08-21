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
package edu.uci.ics.asterix.runtime.formats;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.dataflow.data.nontagged.AqlNullWriterFactory;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryBooleanInspectorImpl;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFamilyProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryIntegerInspector;
import edu.uci.ics.asterix.formats.nontagged.AqlCSVPrinterFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlJSONPrinterFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlNormalizedKeyComputerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlPredicateEvaluatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.FunctionManagerHolder;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.functions.IFunctionManager;
import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarCountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarMaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarMinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarSqlCountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarSqlMaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarSqlMinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarSqlSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.scalar.ScalarSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableCountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableGlobalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableGlobalSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableIntermediateAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableIntermediateSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableLocalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableLocalSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableSqlCountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableSqlSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.AvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.CountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.GlobalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.GlobalSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.IntermediateAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.IntermediateSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalMaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalMinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalSqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalSqlMaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalSqlMinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalSqlSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.MaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.MinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SqlAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SqlCountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SqlMaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SqlMinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SqlSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.stream.EmptyStreamAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.stream.NonEmptyStreamAggregateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.CircleCenterAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.CircleRadiusAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.LineRectanglePolygonAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.PointXCoordinateAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.PointYCoordinateAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalDayAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalHourAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalEndAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalEndDateAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalEndDatetimeAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalEndTimeAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalStartAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalStartDateAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalStartDatetimeAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalIntervalStartTimeAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalMillisecondAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalMinuteAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalMonthAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalSecondAccessor;
import edu.uci.ics.asterix.runtime.evaluators.accessors.TemporalYearAccessor;
import edu.uci.ics.asterix.runtime.evaluators.common.CreateMBREvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.common.FunctionManagerImpl;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ABinaryBase64StringConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ABinaryHexStringConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ABooleanConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ACircleConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADateConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADateTimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADayTimeDurationConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADoubleConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADurationConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AFloatConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt16ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt32ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt64ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt8ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AIntervalFromDateConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AIntervalFromDateTimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AIntervalFromTimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AIntervalStartFromDateConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AIntervalStartFromDateTimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AIntervalStartFromTimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ALineConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ANullConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.APoint3DConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.APointConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.APolygonConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ARectangleConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AStringConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ATimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AUUIDFromStringConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AYearMonthDurationConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ClosedRecordConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.OpenRecordConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.AndDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.AnyCollectionMemberDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CastListDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CastRecordDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CodePointToStringDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.ContainsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CountHashedGramTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CountHashedWordTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateCircleDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateLineDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateMBRDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreatePointDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreatePolygonDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateRectangleDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateUUIDDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceContainsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceListIsFilterable;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceStringIsFilterable;
import edu.uci.ics.asterix.runtime.evaluators.functions.EmbedTypeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EndsWithDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.FlowRecordDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.FuzzyEqDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.GetItemDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.GramTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.HashedGramTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.HashedWordTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.InjectFailureDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.IsNullDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.IsSystemNullDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.LenDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.LikeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NotDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NotNullDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericAbsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericAddDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericCaretDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericCeilingDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericDivideDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericFloorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericModuloDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericMultiplyDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericRoundDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericRoundHalfToEven2Descriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericRoundHalfToEvenDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericSubDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericUnaryMinusDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.OrDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.OrderedListConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.PrefixLenJaccardDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.RegExpDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardPrefixCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardPrefixDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardSortedCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardSortedDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialAreaDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialCellDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialDistanceDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialIntersectDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StartsWithDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringConcatDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringEndWithDescrtiptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringEqualDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringJoinDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringLengthDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringLowerCaseDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringMatchesDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringMatchesWithFlagDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringReplaceDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringReplaceWithFlagsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringStartWithDescrtiptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringToCodePointDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StringUpperCaseDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.Substring2Descriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SubstringAfterDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SubstringBeforeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SubstringDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SwitchCaseDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.UnorderedListConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.WordTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.BinaryConcatDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.BinaryLengthDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.FindBinaryDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.FindBinaryFromDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.ParseBinaryDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.PrintBinaryDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.SubBinaryFromDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.binary.SubBinaryFromToDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.FieldAccessByIndexDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.FieldAccessByIndexEvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.FieldAccessByNameDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.FieldAccessNestedDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.FieldAccessNestedEvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.GetRecordFieldValueDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.GetRecordFieldsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.records.RecordMergeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.AdjustDateTimeForTimeZoneDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.AdjustTimeForTimeZoneDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.CalendarDuartionFromDateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.CalendarDurationFromDateTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.CurrentDateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.CurrentDateTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.CurrentTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DateFromDatetimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DateFromUnixTimeInDaysDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DatetimeFromDateAndTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInMsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInSecsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DayOfWeekDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DayTimeDurationComparatorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DurationEqualDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DurationFromIntervalDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DurationFromMillisecondsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.DurationFromMonthsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.GetDayTimeDurationDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.GetOverlappingIntervalDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.GetYearMonthDurationDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalAfterDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalBeforeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalBinDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalCoveredByDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalCoversDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalEndedByDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalEndsDecriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalMeetsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalMetByDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalOverlappedByDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalOverlapsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalStartedByDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.IntervalStartsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.MillisecondsFromDayTimeDurationDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.MonthsFromYearMonthDurationDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.OverlapBinsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.OverlapDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.ParseDateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.ParseDateTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.ParseTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.PrintDateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.PrintDateTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.PrintTimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.TimeFromDatetimeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.TimeFromUnixTimeInMsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.temporal.YearMonthDurationComparatorDecriptor;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.RangeDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.SubsetCollectionDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NonTaggedDataFormat implements IDataFormat {

    private static boolean registered = false;

    public static final NonTaggedDataFormat INSTANCE = new NonTaggedDataFormat();

    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    private static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<ATypeTag, IValueParserFactory>();

    public static final String NON_TAGGED_DATA_FORMAT = "edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat";

    static {
        typeToValueParserFactMap.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
    }

    public NonTaggedDataFormat() {
    }

    public void registerRuntimeFunctions() throws AlgebricksException {

        if (registered) {
            return;
        }
        registered = true;

        if (FunctionManagerHolder.getFunctionManager() != null) {
            return;
        }

        List<IFunctionDescriptorFactory> temp = new ArrayList<IFunctionDescriptorFactory>();

        // format-independent
        temp.add(ContainsDescriptor.FACTORY);
        temp.add(EndsWithDescriptor.FACTORY);
        temp.add(StartsWithDescriptor.FACTORY);
        temp.add(SubstringDescriptor.FACTORY);
        temp.add(TidRunningAggregateDescriptor.FACTORY);

        // format-dependent
        temp.add(AndDescriptor.FACTORY);
        temp.add(OrDescriptor.FACTORY);
        temp.add(LikeDescriptor.FACTORY);
        temp.add(ScanCollectionDescriptor.FACTORY);
        temp.add(AnyCollectionMemberDescriptor.FACTORY);
        temp.add(ClosedRecordConstructorDescriptor.FACTORY);
        temp.add(FieldAccessByIndexDescriptor.FACTORY);
        temp.add(FieldAccessByNameDescriptor.FACTORY);
        temp.add(FieldAccessNestedDescriptor.FACTORY);
        temp.add(GetRecordFieldsDescriptor.FACTORY);
        temp.add(GetRecordFieldValueDescriptor.FACTORY);
        temp.add(FieldAccessByNameDescriptor.FACTORY);
        temp.add(GetItemDescriptor.FACTORY);
        temp.add(NumericUnaryMinusDescriptor.FACTORY);
        temp.add(OpenRecordConstructorDescriptor.FACTORY);
        temp.add(OrderedListConstructorDescriptor.FACTORY);
        temp.add(UnorderedListConstructorDescriptor.FACTORY);
        temp.add(EmbedTypeDescriptor.FACTORY);

        temp.add(NumericAddDescriptor.FACTORY);
        temp.add(NumericDivideDescriptor.FACTORY);
        temp.add(NumericMultiplyDescriptor.FACTORY);
        temp.add(NumericSubDescriptor.FACTORY);
        temp.add(NumericModuloDescriptor.FACTORY);
        temp.add(NumericCaretDescriptor.FACTORY);
        temp.add(IsNullDescriptor.FACTORY);
        temp.add(IsSystemNullDescriptor.FACTORY);
        temp.add(NotDescriptor.FACTORY);
        temp.add(LenDescriptor.FACTORY);
        temp.add(EmptyStreamAggregateDescriptor.FACTORY);
        temp.add(NonEmptyStreamAggregateDescriptor.FACTORY);
        temp.add(RangeDescriptor.FACTORY);

        temp.add(NumericAbsDescriptor.FACTORY);
        temp.add(NumericCeilingDescriptor.FACTORY);
        temp.add(NumericFloorDescriptor.FACTORY);
        temp.add(NumericRoundDescriptor.FACTORY);
        temp.add(NumericRoundHalfToEvenDescriptor.FACTORY);
        temp.add(NumericRoundHalfToEven2Descriptor.FACTORY);

        // Binary functions
        temp.add(BinaryLengthDescriptor.FACTORY);
        temp.add(ParseBinaryDescriptor.FACTORY);
        temp.add(PrintBinaryDescriptor.FACTORY);
        temp.add(BinaryConcatDescriptor.FACTORY);
        temp.add(SubBinaryFromDescriptor.FACTORY);
        temp.add(SubBinaryFromToDescriptor.FACTORY);
        temp.add(FindBinaryDescriptor.FACTORY);
        temp.add(FindBinaryFromDescriptor.FACTORY);

        // String functions
        temp.add(StringEqualDescriptor.FACTORY);
        temp.add(StringStartWithDescrtiptor.FACTORY);
        temp.add(StringEndWithDescrtiptor.FACTORY);
        temp.add(StringMatchesDescriptor.FACTORY);
        temp.add(StringLowerCaseDescriptor.FACTORY);
        temp.add(StringUpperCaseDescriptor.FACTORY);
        temp.add(StringMatchesWithFlagDescriptor.FACTORY);
        temp.add(StringReplaceDescriptor.FACTORY);
        temp.add(StringReplaceWithFlagsDescriptor.FACTORY);
        temp.add(StringLengthDescriptor.FACTORY);
        temp.add(Substring2Descriptor.FACTORY);
        temp.add(SubstringBeforeDescriptor.FACTORY);
        temp.add(SubstringAfterDescriptor.FACTORY);
        temp.add(StringToCodePointDescriptor.FACTORY);
        temp.add(CodePointToStringDescriptor.FACTORY);
        temp.add(StringConcatDescriptor.FACTORY);
        temp.add(StringJoinDescriptor.FACTORY);

        // aggregates
        temp.add(ListifyAggregateDescriptor.FACTORY);
        temp.add(CountAggregateDescriptor.FACTORY);
        temp.add(AvgAggregateDescriptor.FACTORY);
        temp.add(LocalAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SumAggregateDescriptor.FACTORY);
        temp.add(LocalSumAggregateDescriptor.FACTORY);
        temp.add(MaxAggregateDescriptor.FACTORY);
        temp.add(LocalMaxAggregateDescriptor.FACTORY);
        temp.add(MinAggregateDescriptor.FACTORY);
        temp.add(LocalMinAggregateDescriptor.FACTORY);

        // serializable aggregates
        temp.add(SerializableCountAggregateDescriptor.FACTORY);
        temp.add(SerializableAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSumAggregateDescriptor.FACTORY);

        // scalar aggregates
        temp.add(ScalarCountAggregateDescriptor.FACTORY);
        temp.add(ScalarAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSumAggregateDescriptor.FACTORY);
        temp.add(ScalarMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarMinAggregateDescriptor.FACTORY);

        // SQL aggregates
        temp.add(SqlCountAggregateDescriptor.FACTORY);
        temp.add(SqlAvgAggregateDescriptor.FACTORY);
        temp.add(LocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SqlSumAggregateDescriptor.FACTORY);
        temp.add(LocalSqlSumAggregateDescriptor.FACTORY);
        temp.add(SqlMaxAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMaxAggregateDescriptor.FACTORY);
        temp.add(SqlMinAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMinAggregateDescriptor.FACTORY);

        // SQL serializable aggregates
        temp.add(SerializableSqlCountAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlSumAggregateDescriptor.FACTORY);

        // SQL scalar aggregates
        temp.add(ScalarSqlCountAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlSumAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMinAggregateDescriptor.FACTORY);

        // new functions - constructors
        temp.add(ABooleanConstructorDescriptor.FACTORY);
        temp.add(ANullConstructorDescriptor.FACTORY);
        temp.add(ABinaryHexStringConstructorDescriptor.FACTORY);
        temp.add(ABinaryBase64StringConstructorDescriptor.FACTORY);
        temp.add(AStringConstructorDescriptor.FACTORY);
        temp.add(AInt8ConstructorDescriptor.FACTORY);
        temp.add(AInt16ConstructorDescriptor.FACTORY);
        temp.add(AInt32ConstructorDescriptor.FACTORY);
        temp.add(AInt64ConstructorDescriptor.FACTORY);
        temp.add(AFloatConstructorDescriptor.FACTORY);
        temp.add(ADoubleConstructorDescriptor.FACTORY);
        temp.add(APointConstructorDescriptor.FACTORY);
        temp.add(APoint3DConstructorDescriptor.FACTORY);
        temp.add(ALineConstructorDescriptor.FACTORY);
        temp.add(APolygonConstructorDescriptor.FACTORY);
        temp.add(ACircleConstructorDescriptor.FACTORY);
        temp.add(ARectangleConstructorDescriptor.FACTORY);
        temp.add(ATimeConstructorDescriptor.FACTORY);
        temp.add(ADateConstructorDescriptor.FACTORY);
        temp.add(ADateTimeConstructorDescriptor.FACTORY);
        temp.add(ADurationConstructorDescriptor.FACTORY);
        temp.add(AYearMonthDurationConstructorDescriptor.FACTORY);
        temp.add(ADayTimeDurationConstructorDescriptor.FACTORY);
        temp.add(AUUIDFromStringConstructorDescriptor.FACTORY);

        temp.add(CreateUUIDDescriptor.FACTORY);
        // Spatial
        temp.add(CreatePointDescriptor.FACTORY);
        temp.add(CreateLineDescriptor.FACTORY);
        temp.add(CreatePolygonDescriptor.FACTORY);
        temp.add(CreateCircleDescriptor.FACTORY);
        temp.add(CreateRectangleDescriptor.FACTORY);
        temp.add(SpatialAreaDescriptor.FACTORY);
        temp.add(SpatialDistanceDescriptor.FACTORY);
        temp.add(SpatialIntersectDescriptor.FACTORY);
        temp.add(CreateMBRDescriptor.FACTORY);
        temp.add(SpatialCellDescriptor.FACTORY);
        temp.add(PointXCoordinateAccessor.FACTORY);
        temp.add(PointYCoordinateAccessor.FACTORY);
        temp.add(CircleRadiusAccessor.FACTORY);
        temp.add(CircleCenterAccessor.FACTORY);
        temp.add(LineRectanglePolygonAccessor.FACTORY);

        // fuzzyjoin function
        temp.add(FuzzyEqDescriptor.FACTORY);
        temp.add(SubsetCollectionDescriptor.FACTORY);
        temp.add(PrefixLenJaccardDescriptor.FACTORY);

        temp.add(WordTokensDescriptor.FACTORY);
        temp.add(HashedWordTokensDescriptor.FACTORY);
        temp.add(CountHashedWordTokensDescriptor.FACTORY);

        temp.add(GramTokensDescriptor.FACTORY);
        temp.add(HashedGramTokensDescriptor.FACTORY);
        temp.add(CountHashedGramTokensDescriptor.FACTORY);

        temp.add(EditDistanceDescriptor.FACTORY);
        temp.add(EditDistanceCheckDescriptor.FACTORY);
        temp.add(EditDistanceStringIsFilterable.FACTORY);
        temp.add(EditDistanceListIsFilterable.FACTORY);
        temp.add(EditDistanceContainsDescriptor.FACTORY);

        temp.add(SimilarityJaccardDescriptor.FACTORY);
        temp.add(SimilarityJaccardCheckDescriptor.FACTORY);
        temp.add(SimilarityJaccardSortedDescriptor.FACTORY);
        temp.add(SimilarityJaccardSortedCheckDescriptor.FACTORY);
        temp.add(SimilarityJaccardPrefixDescriptor.FACTORY);
        temp.add(SimilarityJaccardPrefixCheckDescriptor.FACTORY);

        temp.add(RecordMergeDescriptor.FACTORY);
        temp.add(SwitchCaseDescriptor.FACTORY);
        temp.add(RegExpDescriptor.FACTORY);
        temp.add(InjectFailureDescriptor.FACTORY);
        temp.add(CastListDescriptor.FACTORY);
        temp.add(CastRecordDescriptor.FACTORY);
        temp.add(FlowRecordDescriptor.FACTORY);
        temp.add(NotNullDescriptor.FACTORY);

        // Spatial and temporal type accessors
        temp.add(TemporalYearAccessor.FACTORY);
        temp.add(TemporalMonthAccessor.FACTORY);
        temp.add(TemporalDayAccessor.FACTORY);
        temp.add(TemporalHourAccessor.FACTORY);
        temp.add(TemporalMinuteAccessor.FACTORY);
        temp.add(TemporalSecondAccessor.FACTORY);
        temp.add(TemporalMillisecondAccessor.FACTORY);
        temp.add(TemporalIntervalStartAccessor.FACTORY);
        temp.add(TemporalIntervalEndAccessor.FACTORY);
        temp.add(TemporalIntervalStartDateAccessor.FACTORY);
        temp.add(TemporalIntervalEndDateAccessor.FACTORY);
        temp.add(TemporalIntervalStartTimeAccessor.FACTORY);
        temp.add(TemporalIntervalEndTimeAccessor.FACTORY);
        temp.add(TemporalIntervalStartDatetimeAccessor.FACTORY);
        temp.add(TemporalIntervalEndDatetimeAccessor.FACTORY);

        // Temporal functions
        temp.add(DateFromUnixTimeInDaysDescriptor.FACTORY);
        temp.add(DateFromDatetimeDescriptor.FACTORY);
        temp.add(TimeFromUnixTimeInMsDescriptor.FACTORY);
        temp.add(TimeFromDatetimeDescriptor.FACTORY);
        temp.add(DatetimeFromUnixTimeInMsDescriptor.FACTORY);
        temp.add(DatetimeFromUnixTimeInSecsDescriptor.FACTORY);
        temp.add(DatetimeFromDateAndTimeDescriptor.FACTORY);
        temp.add(CalendarDurationFromDateTimeDescriptor.FACTORY);
        temp.add(CalendarDuartionFromDateDescriptor.FACTORY);
        temp.add(AdjustDateTimeForTimeZoneDescriptor.FACTORY);
        temp.add(AdjustTimeForTimeZoneDescriptor.FACTORY);
        temp.add(IntervalBeforeDescriptor.FACTORY);
        temp.add(IntervalAfterDescriptor.FACTORY);
        temp.add(IntervalMeetsDescriptor.FACTORY);
        temp.add(IntervalMetByDescriptor.FACTORY);
        temp.add(IntervalOverlapsDescriptor.FACTORY);
        temp.add(IntervalOverlappedByDescriptor.FACTORY);
        temp.add(OverlapDescriptor.FACTORY);
        temp.add(IntervalStartsDescriptor.FACTORY);
        temp.add(IntervalStartedByDescriptor.FACTORY);
        temp.add(IntervalCoversDescriptor.FACTORY);
        temp.add(IntervalCoveredByDescriptor.FACTORY);
        temp.add(IntervalEndsDecriptor.FACTORY);
        temp.add(IntervalEndedByDescriptor.FACTORY);
        temp.add(CurrentDateDescriptor.FACTORY);
        temp.add(CurrentTimeDescriptor.FACTORY);
        temp.add(CurrentDateTimeDescriptor.FACTORY);
        temp.add(DurationFromMillisecondsDescriptor.FACTORY);
        temp.add(DurationFromMonthsDescriptor.FACTORY);
        temp.add(YearMonthDurationComparatorDecriptor.GREATER_THAN_FACTORY);
        temp.add(YearMonthDurationComparatorDecriptor.LESS_THAN_FACTORY);
        temp.add(DayTimeDurationComparatorDescriptor.GREATER_THAN_FACTORY);
        temp.add(DayTimeDurationComparatorDescriptor.LESS_THAN_FACTORY);
        temp.add(MonthsFromYearMonthDurationDescriptor.FACTORY);
        temp.add(MillisecondsFromDayTimeDurationDescriptor.FACTORY);
        temp.add(DurationEqualDescriptor.FACTORY);
        temp.add(GetYearMonthDurationDescriptor.FACTORY);
        temp.add(GetDayTimeDurationDescriptor.FACTORY);
        temp.add(IntervalBinDescriptor.FACTORY);
        temp.add(OverlapBinsDescriptor.FACTORY);
        temp.add(DayOfWeekDescriptor.FACTORY);
        temp.add(ParseDateDescriptor.FACTORY);
        temp.add(ParseTimeDescriptor.FACTORY);
        temp.add(ParseDateTimeDescriptor.FACTORY);
        temp.add(PrintDateDescriptor.FACTORY);
        temp.add(PrintTimeDescriptor.FACTORY);
        temp.add(PrintDateTimeDescriptor.FACTORY);
        temp.add(GetOverlappingIntervalDescriptor.FACTORY);
        temp.add(DurationFromIntervalDescriptor.FACTORY);

        // Interval constructor
        temp.add(AIntervalFromDateConstructorDescriptor.FACTORY);
        temp.add(AIntervalFromTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalFromDateTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromDateConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromDateTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromTimeConstructorDescriptor.FACTORY);

        IFunctionManager mgr = new FunctionManagerImpl();
        for (IFunctionDescriptorFactory fdFactory : temp) {
            mgr.registerFunction(fdFactory);
        }
        FunctionManagerHolder.setFunctionManager(mgr);
    }

    @Override
    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory() {
        return AqlBinaryBooleanInspectorImpl.FACTORY;
    }

    @Override
    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return AqlBinaryComparatorFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return AqlBinaryHashFunctionFactoryProvider.INSTANCE;
    }

    @Override
    public ISerializerDeserializerProvider getSerdeProvider() {
        return AqlSerializerDeserializerProvider.INSTANCE; // done
    }

    @Override
    public ITypeTraitProvider getTypeTraitProvider() {
        return AqlTypeTraitProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ICopyEvaluatorFactory getFieldAccessEvaluatorFactory(ARecordType recType, List<String> fldName,
            int recordColumn) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        boolean fieldFound = false;
        ICopyEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(recordColumn);
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        ICopyEvaluatorFactory evalFactory = null;
        if (fldName.size() == 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    fieldFound = true;
                    try {
                        AInt32 ai = new AInt32(i);
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(
                                ai, dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    ICopyEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(Arrays.copyOf(
                            abvs.getByteArray(), abvs.getLength()));

                    evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory, fldIndexEvalFactory, recType);
                    return evalFactory;
                }
            }
        }
        if (fldName.size() > 1 || (!fieldFound && recType.isOpen())) {
            if (fldName.size() == 1) {
                AString as = new AString(fldName.get(0));
                try {
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            } else {
                AOrderedList as = new AOrderedList(fldName);
                try {
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            ICopyEvaluatorFactory[] factories = new ICopyEvaluatorFactory[2];
            factories[0] = recordEvalFactory;
            if (fldName.size() > 1) {
                evalFactory = new FieldAccessNestedEvalFactory(recordEvalFactory, recType, fldName);
            } else {
                evalFactory = FieldAccessByNameDescriptor.FACTORY.createFunctionDescriptor().createEvaluatorFactory(
                        factories);
            }
            return evalFactory;
        } else
            throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public ICopyEvaluatorFactory[] createMBRFactory(ARecordType recType, List<String> fldName, int recordColumn,
            int dimension, List<String> filterFieldName) throws AlgebricksException {
        ICopyEvaluatorFactory evalFactory = getFieldAccessEvaluatorFactory(recType, fldName, recordColumn);
        int numOfFields = dimension * 2;
        ICopyEvaluatorFactory[] evalFactories = new ICopyEvaluatorFactory[numOfFields
                + (filterFieldName == null ? 0 : 1)];

        ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
        DataOutput dos1 = abvs1.getDataOutput();
        try {
            AInt32 ai = new AInt32(dimension);
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos1);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        ICopyEvaluatorFactory dimensionEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs1.getByteArray(),
                abvs1.getLength()));

        for (int i = 0; i < numOfFields; i++) {
            ArrayBackedValueStorage abvs2 = new ArrayBackedValueStorage();
            DataOutput dos2 = abvs2.getDataOutput();
            try {
                AInt32 ai = new AInt32(i);
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos2);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            ICopyEvaluatorFactory coordinateEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs2.getByteArray(),
                    abvs2.getLength()));

            evalFactories[i] = new CreateMBREvalFactory(evalFactory, dimensionEvalFactory, coordinateEvalFactory);
        }
        if (filterFieldName != null) {
            evalFactories[numOfFields] = getFieldAccessEvaluatorFactory(recType, filterFieldName, recordColumn);
        }
        return evalFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            ARecordType recType, List<String> fldName) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        if (fldName.size() > 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    ICopyEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                            GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
                    ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                    DataOutput dos = abvs.getDataOutput();
                    try {
                        AInt32 ai = new AInt32(i);
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(
                                ai, dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    ICopyEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(Arrays.copyOf(
                            abvs.getByteArray(), abvs.getLength()));
                    ICopyEvaluatorFactory evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory,
                            fldIndexEvalFactory, recType);
                    IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                            .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX);

                    ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                            new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                    new AInt32(i)))));
                    return new Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory,
                            partitionFun, recType.getFieldTypes()[i]);
                }
            }
        } else {
            ICopyEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                    GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            DataOutput dos = abvs.getDataOutput();
            AOrderedList as = new AOrderedList(fldName);
            try {
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as, dos);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            ICopyEvaluatorFactory evalFactory = new FieldAccessNestedEvalFactory(recordEvalFactory, recType, fldName);
            IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                    .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED);

            ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(as))));
            try {
                return new Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory,
                        partitionFun, recType.getSubFieldType(fldName));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @Override
    public IFunctionDescriptor resolveFunction(ILogicalExpression expr, IVariableTypeEnvironment context)
            throws AlgebricksException {
        FunctionIdentifier fnId = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier();
        IFunctionManager mgr = FunctionManagerHolder.getFunctionManager();
        IFunctionDescriptor fd = mgr.lookupFunction(fnId);
        if (fd == null) {
            throw new AsterixRuntimeException("Unresolved function " + fnId);
        }
        typeInference(expr, fd, context);
        return fd;
    }

    private void typeInference(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
            throws AlgebricksException {
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.LISTIFY)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            if (f.getArguments().size() == 0) {
                ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(null, null));
            } else {
                IAType itemType = (IAType) context.getType(f.getArguments().get(0).getValue());
                if (itemType instanceof AUnionType) {
                    if (((AUnionType) itemType).isNullableType())
                        itemType = ((AUnionType) itemType).getNullableType();
                    else
                        // Convert UNION types into ANY.
                        itemType = BuiltinType.ANY;
                }
                ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(itemType, null));
            }
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.RECORD_MERGE)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
            ((RecordMergeDescriptor) fd).reset(outType, type0, type1);
        }

        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CAST_RECORD)) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            ARecordType rt = (ARecordType) TypeComputerUtilities.getRequiredType(funcExpr);
            IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
            if (it.getTypeTag().equals(ATypeTag.ANY)) {
                it = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            ((CastRecordDescriptor) fd).reset(rt, (ARecordType) it);
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CAST_LIST)) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            AbstractCollectionType rt = (AbstractCollectionType) TypeComputerUtilities.getRequiredType(funcExpr);
            IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
            if (it.getTypeTag().equals(ATypeTag.ANY)) {
                it = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            ((CastListDescriptor) fd).reset(rt, (AbstractCollectionType) it);
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FLOW_RECORD)) {
            ARecordType it = (ARecordType) TypeComputerUtilities.getInputType((AbstractFunctionCallExpression) expr);
            ((FlowRecordDescriptor) fd).reset(it);
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
            ARecordType rt = (ARecordType) context.getType(expr);
            ((OpenRecordConstructorDescriptor) fd).reset(rt,
                    computeOpenFields((AbstractFunctionCallExpression) expr, rt));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            ((ClosedRecordConstructorDescriptor) fd).reset((ARecordType) context.getType(expr));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)) {
            ((OrderedListConstructorDescriptor) fd).reset((AOrderedListType) context.getType(expr));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR)) {
            ((UnorderedListConstructorDescriptor) fd).reset((AUnorderedListType) context.getType(expr));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            switch (t.getTypeTag()) {
                case RECORD: {
                    ARecordType recType = (ARecordType) t;
                    ((FieldAccessByIndexDescriptor) fd).reset(recType);
                    break;
                }
                case UNION: {
                    AUnionType unionT = (AUnionType) t;
                    if (unionT.isNullableType()) {
                        IAType t2 = unionT.getNullableType();
                        if (t2.getTypeTag() == ATypeTag.RECORD) {
                            ARecordType recType = (ARecordType) t2;
                            ((FieldAccessByIndexDescriptor) fd).reset(recType);
                            break;
                        }
                    }
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
                default: {
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
            }
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            AOrderedList fieldPath = (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) fce.getArguments()
                    .get(1).getValue()).getValue()).getObject());
            List<String> listFieldPath = new ArrayList<String>();
            for (int i = 0; i < fieldPath.size(); i++) {
                listFieldPath.add(((AString) fieldPath.getItem(i)).getStringValue());
            }

            switch (t.getTypeTag()) {
                case RECORD: {
                    ARecordType recType = (ARecordType) t;
                    ((FieldAccessNestedDescriptor) fd).reset(recType, listFieldPath);
                    break;
                }
                default: {
                    throw new NotImplementedException("field-access-nested for data of type " + t);
                }
            }
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.GET_RECORD_FIELDS)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            if (t.getTypeTag().equals(ATypeTag.RECORD)) {
                ARecordType recType = (ARecordType) t;
                ((GetRecordFieldsDescriptor) fd).reset(recType);
            } else {
                throw new NotImplementedException("get-record-fields for data of type " + t);
            }
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.GET_RECORD_FIELD_VALUE)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            if (t.getTypeTag().equals(ATypeTag.RECORD)) {
                ARecordType recType = (ARecordType) t;
                ((GetRecordFieldValueDescriptor) fd).reset(recType);
            } else {
                throw new NotImplementedException("get-record-field-value for data of type " + t);
            }
        }
    }

    private boolean[] computeOpenFields(AbstractFunctionCallExpression expr, ARecordType recType) {
        int n = expr.getArguments().size() / 2;
        boolean[] open = new boolean[n];
        for (int i = 0; i < n; i++) {
            Mutable<ILogicalExpression> argRef = expr.getArguments().get(2 * i);
            ILogicalExpression arg = argRef.getValue();
            if (arg.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                String fn = ((AString) ((AsterixConstantValue) ((ConstantExpression) arg).getValue()).getObject())
                        .getStringValue();
                open[i] = true;
                for (String s : recType.getFieldNames()) {
                    if (s.equals(fn)) {
                        open[i] = false;
                        break;
                    }
                }
            } else {
                open[i] = true;
            }
        }
        return open;
    }

    @Override
    public IPrinterFactoryProvider getPrinterFactoryProvider() {
        return AqlPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getJSONPrinterFactoryProvider() {
        return AqlJSONPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getCSVPrinterFactoryProvider() {
        return AqlCSVPrinterFactoryProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ICopyEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException {
        IAObject obj = null;
        if (value.isNull()) {
            obj = ANull.NULL;
        } else if (value.isTrue()) {
            obj = ABoolean.TRUE;
        } else if (value.isFalse()) {
            obj = ABoolean.FALSE;
        } else {
            AsterixConstantValue acv = (AsterixConstantValue) value;
            obj = acv.getObject();
        }
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        try {
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(obj.getType()).serialize(obj, dos);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
    }

    @Override
    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory() {
        return AqlBinaryIntegerInspector.FACTORY;
    }

    @Override
    public ITupleParserFactory createTupleParser(ARecordType recType, IParseFileSplitsDecl decl) {
        return createTupleParser(recType, decl.isDelimitedFileFormat(), decl.getDelimChar(), decl.getQuote(),
                decl.getHasHeader());
    }

    @Override
    public ITupleParserFactory createTupleParser(ARecordType recType, boolean delimitedFormat, char delimiter,
            char quote, boolean hasHeader) {
        Map<String, String> conf = new HashMap<String, String>();
        AsterixTupleParserFactory.InputDataFormat inputFormat = null;
        if (delimitedFormat) {
            conf.put(AsterixTupleParserFactory.KEY_FORMAT, AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT);
            conf.put(AsterixTupleParserFactory.KEY_DELIMITER, "" + delimiter);
            inputFormat = InputDataFormat.DELIMITED;
        } else {
            conf.put(AsterixTupleParserFactory.KEY_FORMAT, AsterixTupleParserFactory.FORMAT_ADM);
            inputFormat = InputDataFormat.ADM;
        }

        if (hasHeader) {
            conf.put(AsterixTupleParserFactory.HAS_HEADER,
                    hasHeader ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
        }
        conf.put(AsterixTupleParserFactory.KEY_QUOTE, "" + quote);
        return new AsterixTupleParserFactory(conf, recType, inputFormat);
    }


    @Override
    public INullWriterFactory getNullWriterFactory() {
        return AqlNullWriterFactory.INSTANCE;
    }

    @Override
    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return new IExpressionEvalSizeComputer() {
            @Override
            public int getEvalSize(ILogicalExpression expr, IVariableEvalSizeEnvironment env)
                    throws AlgebricksException {
                switch (expr.getExpressionTag()) {
                    case CONSTANT: {
                        ConstantExpression c = (ConstantExpression) expr;
                        if (c == ConstantExpression.NULL) {
                            return 1;
                        } else if (c == ConstantExpression.FALSE || c == ConstantExpression.TRUE) {
                            return 2;
                        } else {
                            AsterixConstantValue acv = (AsterixConstantValue) c.getValue();
                            IAObject o = acv.getObject();
                            switch (o.getType().getTypeTag()) {
                                case DOUBLE: {
                                    return 9;
                                }
                                case BOOLEAN: {
                                    return 2;
                                }
                                case NULL: {
                                    return 1;
                                }
                                case INT32: {
                                    return 5;
                                }
                                case INT64: {
                                    return 9;
                                }
                                default: {
                                    // TODO
                                    return -1;
                                }
                            }
                        }
                    }
                    case FUNCTION_CALL: {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.TID)) {
                            return 5;
                        } else {
                            // TODO
                            return -1;
                        }
                    }
                    default: {
                        // TODO
                        return -1;
                    }
                }
            }
        };
    }

    @Override
    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return AqlNormalizedKeyComputerFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider() {
        return AqlBinaryHashFunctionFamilyProvider.INSTANCE;
    }

    @Override
    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider() {
        return AqlPredicateEvaluatorFactoryProvider.INSTANCE;
    }

}
