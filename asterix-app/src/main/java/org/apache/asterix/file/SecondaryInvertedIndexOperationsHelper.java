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

package org.apache.asterix.file;

import java.util.List;

import org.apache.asterix.common.api.ILocalResourceMetadata;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.DatasetConfig.IndexTypeProperty;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.resource.LSMInvertedIndexLocalResourceMetadata;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexCompactOperator;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.PartitionedLSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;

public class SecondaryInvertedIndexOperationsHelper extends SecondaryIndexOperationsHelper {

    private IAType secondaryKeyType;
    private ITypeTraits[] invListsTypeTraits;
    private IBinaryComparatorFactory[] invListComparatorFactories;
    private IBinaryComparatorFactory[] tokenComparatorFactories;
    private ITypeTraits[] tokenTypeTraits;
    private IBinaryTokenizerFactory tokenizerFactory;
    // For tokenization, sorting and loading. Represents <token, primary keys>.
    private int numTokenKeyPairFields;
    private IBinaryComparatorFactory[] tokenKeyPairComparatorFactories;
    private RecordDescriptor tokenKeyPairRecDesc;
    private boolean isPartitioned;
    private int[] invertedIndexFields;
    private int[] invertedIndexFieldsForNonBulkLoadOps;
    private int[] secondaryFilterFieldsForNonBulkLoadOps;
    private RecordDescriptor assignOpRecDesc;

    protected SecondaryInvertedIndexOperationsHelper(PhysicalOptimizationConfig physOptConf,
            IAsterixPropertiesProvider propertiesProvider, IndexType indexType) {
        super(physOptConf, propertiesProvider, indexType);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void setSecondaryRecDescAndComparators(IndexTypeProperty indexTypeProperty,
            List<List<String>> secondaryKeyFields, List<IAType> secondaryKeyTypes, AqlMetadataProvider metadata)
            throws AlgebricksException, AsterixException {
        // Sanity checks.
        if (numPrimaryKeys > 2 || (numPrimaryKeys > 1 && indexType != IndexType.SIF)) {
            throw new AsterixException("Cannot create inverted index on dataset with composite primary key.");
        }
        if (numSecondaryKeys > 1) {
            throw new AsterixException("Cannot create composite inverted index on multiple fields.");
        }
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }
        // Prepare record descriptor used in the assign op, and the optional
        // select op.

        ITypeTraits[] assignOpTypeTraits = null;
        ISerializerDeserializer[] assignOpRecFieldsSerde = null;
        int assignOpOutputFieldsCount = numSecondaryKeys + numPrimaryKeys + numFilterFields;
        if (indexType == IndexType.SIF) {
            ++assignOpOutputFieldsCount; //+1 for original point
            assignOpTypeTraits = new ITypeTraits[assignOpOutputFieldsCount];
            assignOpRecFieldsSerde = new ISerializerDeserializer[assignOpOutputFieldsCount];
        }
        int evalFactoriesCount = assignOpOutputFieldsCount - numPrimaryKeys;
        secondaryFieldAccessEvalFactories = new ICopyEvaluatorFactory[evalFactoriesCount];
        ISerializerDeserializer[] secondaryRecFields = new ISerializerDeserializer[assignOpOutputFieldsCount];
        ISerializerDeserializer[] enforcedRecFields = new ISerializerDeserializer[1 + numPrimaryKeys + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[assignOpOutputFieldsCount - numFilterFields];
        ITypeTraits[] enforcedTypeTraits = new ITypeTraits[1 + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = FormatUtils.getDefaultFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = FormatUtils.getDefaultFormat().getTypeTraitProvider();

        int i = 0;
        for (; i < numSecondaryKeys; i++) {
            secondaryFieldAccessEvalFactories[i] = FormatUtils.getDefaultFormat().getFieldAccessEvaluatorFactory(
                    isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFields.get(i), numPrimaryKeys);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(i),
                    secondaryKeyFields.get(i), itemType);
            secondaryKeyType = keyTypePair.first;
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(secondaryKeyType);
            secondaryRecFields[i] = keySerde;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(secondaryKeyType);
            if (indexType == IndexType.SIF) {
                assignOpTypeTraits[i] = typeTraitProvider.getTypeTrait(secondaryKeyType);
                assignOpRecFieldsSerde[i] = keySerde;
            }
        }
        
        if (indexType == IndexType.SIF) {
            for (int j = 0; j < numPrimaryKeys; j++, i++) {
                assignOpTypeTraits[i] = primaryRecDesc.getTypeTraits()[j];
                assignOpRecFieldsSerde[i] = primaryRecDesc.getFields()[j];
            }
        }
        
        if (indexType == IndexType.SIF) {
            //for the original point
            secondaryFieldAccessEvalFactories[i - numPrimaryKeys] = FormatUtils.getDefaultFormat().getFieldAccessEvaluatorFactory(
                    isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFields.get(0), numPrimaryKeys);
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(BuiltinType.APOINT);
            secondaryRecFields[i] = keySerde;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);
            if (indexType == IndexType.SIF) {
                assignOpTypeTraits[i] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);
                assignOpRecFieldsSerde[i] = keySerde;
            }
            i++;
        }
        
        if (numFilterFields > 0) {
            secondaryFieldAccessEvalFactories[evalFactoriesCount-numFilterFields] = FormatUtils.getDefaultFormat().getFieldAccessEvaluatorFactory(
                    itemType, filterFieldName, numPrimaryKeys);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(filterFieldName, itemType);
            IAType type = keyTypePair.first;
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(type);
            int idx = numPrimaryKeys + numSecondaryKeys;
            if (indexType == IndexType.SIF) {
                ++idx;
                assignOpRecFieldsSerde[idx] = serde;
                secondaryRecFields[idx] = serde;
            } else {
                secondaryRecFields[idx] = serde;
            }
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields);
        if (indexType == IndexType.SIF) {
            assignOpRecDesc = new RecordDescriptor(assignOpRecFieldsSerde, assignOpTypeTraits);
        }

        // Comparators and type traits for tokens.
        int numTokenFields = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
        tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
        tokenTypeTraits = new ITypeTraits[numTokenFields];
        tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
        if (isPartitioned) {
            // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
            tokenComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
            tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
        }
        // Set tokenizer factory.
        // TODO: We might want to expose the hashing option at the AQL level,
        // and add the choice to the index metadata.
        tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(secondaryKeyType.getTypeTag(), indexType,
                indexTypeProperty);
        // Type traits for inverted-list elements. Inverted lists contain
        // primary keys (and addition fields if any such as original point for sif index).
        int nonTokenFieldsCount = numPrimaryKeys;
        if (indexType == IndexType.SIF) {
            nonTokenFieldsCount++;
        }
        invListsTypeTraits = new ITypeTraits[nonTokenFieldsCount];
        invListComparatorFactories = new IBinaryComparatorFactory[nonTokenFieldsCount];
        for (i = 0; i < numPrimaryKeys; i++) {
            invListsTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
            enforcedRecFields[i] = primaryRecDesc.getFields()[i];
            enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
        }
        enforcedRecFields[i] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);
        
        if (indexType == IndexType.SIF) {
            invListsTypeTraits[i] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);
            i++;
        }

        // For tokenization, sorting and loading.
        // One token (+ optional partitioning field) + primary keys.
        numTokenKeyPairFields = (!isPartitioned) ? 1 + numPrimaryKeys : 2 + numPrimaryKeys;
        if (indexType == IndexType.SIF) {
            numTokenKeyPairFields++; //for original point field
        }
        ISerializerDeserializer[] tokenKeyPairFields = new ISerializerDeserializer[numTokenKeyPairFields
                + numFilterFields];
        ITypeTraits[] tokenKeyPairTypeTraits = new ITypeTraits[numTokenKeyPairFields];
        tokenKeyPairComparatorFactories = new IBinaryComparatorFactory[numTokenKeyPairFields];
        tokenKeyPairFields[0] = serdeProvider.getSerializerDeserializer(NonTaggedFormatUtil
                .getTokenType(secondaryKeyType));
        tokenKeyPairTypeTraits[0] = tokenTypeTraits[0];
        tokenKeyPairComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        int pkOff = 1;
        if (isPartitioned) {
            tokenKeyPairFields[1] = ShortSerializerDeserializer.INSTANCE;
            tokenKeyPairTypeTraits[1] = tokenTypeTraits[1];
            tokenKeyPairComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
            pkOff = 2;
        }
        for (int j = 0; j < numPrimaryKeys; j++, pkOff++) {
            tokenKeyPairFields[pkOff] = primaryRecDesc.getFields()[j];
            tokenKeyPairTypeTraits[pkOff] = primaryRecDesc.getTypeTraits()[j];
            tokenKeyPairComparatorFactories[pkOff] = primaryComparatorFactories[j];
        }
        if (indexType == IndexType.SIF) {
            tokenKeyPairFields[pkOff] = serdeProvider.getSerializerDeserializer(BuiltinType.APOINT);
            tokenKeyPairTypeTraits[pkOff] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);
            tokenKeyPairComparatorFactories[pkOff] = FormatUtils.getDefaultFormat()
                    .getBinaryComparatorFactoryProvider().getBinaryComparatorFactory(BuiltinType.APOINT, true);
            ++pkOff;
        }
        
        //set invListComparatorFactories using tokenKeyPairComparatorFactories.
        i = numSecondaryKeys + (isPartitioned ? 1 : 0);
        for (int j = 0; i < tokenKeyPairComparatorFactories.length; j++, i++) {
            invListComparatorFactories[j] = tokenKeyPairComparatorFactories[i];
        }
        
        if (numFilterFields > 0) {
            int idx = numPrimaryKeys + numSecondaryKeys;
            if (indexType == IndexType.SIF) {
                ++idx;
            }
            tokenKeyPairFields[pkOff] = secondaryRecFields[idx];
        }
        tokenKeyPairRecDesc = new RecordDescriptor(tokenKeyPairFields, tokenKeyPairTypeTraits);
        if (filterFieldName != null) {
            invertedIndexFields = new int[numTokenKeyPairFields];
            for (i = 0; i < invertedIndexFields.length; i++) {
                invertedIndexFields[i] = i;
            }
            secondaryFilterFieldsForNonBulkLoadOps = new int[numFilterFields];
            int fieldCount = numPrimaryKeys + numSecondaryKeys;
            if (indexType == IndexType.SIF) {
                ++fieldCount;
            }
            secondaryFilterFieldsForNonBulkLoadOps[0] = fieldCount;
            invertedIndexFieldsForNonBulkLoadOps = new int[fieldCount];
            for (i = 0; i < invertedIndexFieldsForNonBulkLoadOps.length; i++) {
                invertedIndexFieldsForNonBulkLoadOps[i] = i;
            }
        }

    }

    @Override
    protected int getNumSecondaryKeys() {
        return numTokenKeyPairFields - numPrimaryKeys;
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
        ILocalResourceMetadata localResourceMetadata = new LSMInvertedIndexLocalResourceMetadata(invListsTypeTraits,
                primaryComparatorFactories, tokenTypeTraits, tokenComparatorFactories, tokenizerFactory, isPartitioned,
                dataset.getDatasetId(), mergePolicyFactory, mergePolicyFactoryProperties, filterTypeTraits,
                filterCmpFactories, invertedIndexFields, secondaryFilterFields, secondaryFilterFieldsForNonBulkLoadOps,
                invertedIndexFieldsForNonBulkLoadOps);
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMInvertedIndexResource);

        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        LSMInvertedIndexCreateOperatorDescriptor invIndexCreateOp = new LSMInvertedIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, tokenTypeTraits, tokenComparatorFactories,
                invListsTypeTraits, invListComparatorFactories, tokenizerFactory, dataflowHelperFactory,
                localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(invIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        // Create dummy key provider for feeding the primary index scan.
        AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);

        // Create primary index scan op.
        BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

        // Create castOp if necessary
        AbstractOperatorDescriptor sourceOp = primaryScanOp;
        if (isEnforcingKeyTypes) {
            sourceOp = createCastOp(spec, primaryScanOp, numSecondaryKeys, dataset.getDatasetType());
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
        }

        // Create assignOp
        AlgebricksMetaOperatorDescriptor asterixAssignOp = null;
        if (indexType == IndexType.SIF) {
            asterixAssignOp = createAssignOpForSIF(spec, sourceOp, numSecondaryKeys);
        } else {
            asterixAssignOp = createAssignOp(spec, sourceOp, numSecondaryKeys);
        }

        // If any of the secondary fields are nullable, then add a select op
        // that filters nulls.
        AlgebricksMetaOperatorDescriptor selectOp = null;
        if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
            selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
        }

        // Create a tokenizer op.
        AbstractOperatorDescriptor tokenizerOp = null;
        
        if (indexType == IndexType.SIF) {
            tokenizerOp = createTokenizerOpForSIF(spec);
        } else {
            tokenizerOp = createTokenizerOp(spec);
        }

        // Sort by token + primary keys.
        ExternalSortOperatorDescriptor sortOp = createSortOp(spec, tokenKeyPairComparatorFactories, tokenKeyPairRecDesc);

        // Create secondary inverted index bulk load op.
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = createInvertedIndexBulkLoadOp(spec);

        AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { new SinkRuntimeFactory() }, new RecordDescriptor[] {});
        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
        if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
            spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, tokenizerOp, 0);
        } else {
            spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, tokenizerOp, 0);
        }
        spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, invIndexBulkLoadOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), invIndexBulkLoadOp, 0, metaOp, 0);
        spec.addRoot(metaOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    private AlgebricksMetaOperatorDescriptor createAssignOpForSIF(JobSpecification spec,
            AbstractOperatorDescriptor sourceOp, int numSecondaryKeys) {
        //*** input and output of assignOp for sif index ****
        //input of assignOp: [PK, RECORD]
        //output of assignOp: [point (APOINT), PK, point (APOINT)], 
        //where the second point is used to store the original point for possible index-only plan.
        //***************************************************
        int[] outColumns = new int[numSecondaryKeys + numFilterFields + 1]; /* +1 for original point */
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys + numFilterFields + 1]; /* +1 for original point */
        for (int i = 0; i < numSecondaryKeys + numFilterFields + 1; i++) { /* +1 for original point */
            outColumns[i] = numPrimaryKeys + i;
        }
        int projCount = 0;
        for (int i = 0; i < numSecondaryKeys; i++) {
            projectionList[projCount++] = numPrimaryKeys + i;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }
        projectionList[projCount++] = outColumns[numSecondaryKeys]; /* this is for the original point */
        if (numFilterFields > 0) {
            projectionList[projCount++] = numPrimaryKeys + numSecondaryKeys + 1;
        }

        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        for (int i = 0; i < secondaryFieldAccessEvalFactories.length; ++i) {
            sefs[i] = new LogicalExpressionJobGenToExpressionRuntimeProviderAdapter.ScalarEvaluatorFactoryAdapter(
                    secondaryFieldAccessEvalFactories[i]);
        }
        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { assignOpRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                primaryPartitionConstraint);
        return asterixAssignOp;
    }

    private AbstractOperatorDescriptor createTokenizerOp(JobSpecification spec) throws AlgebricksException {
        int docField = 0;
        int[] primaryKeyFields = new int[numPrimaryKeys + numFilterFields];
        for (int i = 0; i < primaryKeyFields.length; i++) {
            primaryKeyFields[i] = numSecondaryKeys + i;
        }
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                tokenKeyPairRecDesc, tokenizerFactory, docField, primaryKeyFields, isPartitioned, false, 1, false);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                primaryPartitionConstraint);
        return tokenizerOp;
    }

    private AbstractOperatorDescriptor createTokenizerOpForSIF(JobSpecification spec) {
        int docField = 0;
        int[] invListFields = new int[numPrimaryKeys + 1 + numFilterFields]; //+1 for original point
        for (int i = 0; i < invListFields.length; i++) {
            invListFields[i] = numSecondaryKeys + i;
        }
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec,
                tokenKeyPairRecDesc, tokenizerFactory, docField, invListFields, isPartitioned, false, 1, false);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                primaryPartitionConstraint);
        return tokenizerOp;
    }


    @Override
    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        // Sort on token and primary keys.
        int[] sortFields = new int[numTokenKeyPairFields];
        for (int i = 0; i < numTokenKeyPairFields; i++) {
            sortFields[i] = i;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physOptConf.getMaxFramesExternalSort(), sortFields, tokenKeyPairComparatorFactories, secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    private LSMInvertedIndexBulkLoadOperatorDescriptor createInvertedIndexBulkLoadOp(JobSpecification spec) {
        int[] fieldPermutation = new int[numTokenKeyPairFields + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        LSMInvertedIndexBulkLoadOperatorDescriptor invIndexBulkLoadOp = new LSMInvertedIndexBulkLoadOperatorDescriptor(
                spec, secondaryRecDesc, fieldPermutation, false, numElementsHint, false,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, tokenTypeTraits, tokenComparatorFactories,
                invListsTypeTraits, invListComparatorFactories, tokenizerFactory, dataflowHelperFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, invIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return invIndexBulkLoadOp;
    }

    private IIndexDataflowHelperFactory createDataflowHelperFactory() {
        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        boolean temp = dataset.getDatasetDetails().isTemp();
        if (!isPartitioned) {
            return new LSMInvertedIndexDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                    dataset.getDatasetId()), mergePolicyFactory, mergePolicyFactoryProperties,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                    filterCmpFactories, secondaryFilterFields, secondaryFilterFieldsForNonBulkLoadOps,
                    invertedIndexFieldsForNonBulkLoadOps, !temp);
        } else {
            return new PartitionedLSMInvertedIndexDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                    dataset.getDatasetId()), mergePolicyFactory, mergePolicyFactoryProperties,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), invertedIndexFields, filterTypeTraits,
                    filterCmpFactories, secondaryFilterFields, secondaryFilterFieldsForNonBulkLoadOps,
                    invertedIndexFieldsForNonBulkLoadOps, !temp);
        }
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        IIndexDataflowHelperFactory dataflowHelperFactory = createDataflowHelperFactory();
        LSMInvertedIndexCompactOperator compactOp = new LSMInvertedIndexCompactOperator(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, tokenTypeTraits, tokenComparatorFactories,
                invListsTypeTraits, invListComparatorFactories, tokenizerFactory, dataflowHelperFactory,
                NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);

        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
