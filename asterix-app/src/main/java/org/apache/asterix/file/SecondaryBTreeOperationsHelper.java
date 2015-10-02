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
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.DatasetConfig.IndexTypeProperty;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.external.IndexingConstants;
import org.apache.asterix.metadata.feeds.ExternalDataScanOperatorDescriptor;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.evaluators.functions.ComputeInt64HilbertValueEvalFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.resource.ExternalBTreeWithBuddyLocalResourceMetadata;
import org.apache.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.BinaryTokenizerOperatorDescriptor;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;

public class SecondaryBTreeOperationsHelper extends SecondaryIndexOperationsHelper {

    private IndexTypeProperty indexTypeProperty;
    private RecordDescriptor assignOpRecDesc;
    
    protected SecondaryBTreeOperationsHelper(PhysicalOptimizationConfig physOptConf,
            IAsterixPropertiesProvider propertiesProvider, IndexType indexType) {
        super(physOptConf, propertiesProvider, indexType);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        ILocalResourceFactoryProvider localResourceFactoryProvider;
        IIndexDataflowHelperFactory indexDataflowHelperFactory;

        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
            ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(secondaryTypeTraits,
                    secondaryComparatorFactories, secondaryBloomFilterKeyFields, false, dataset.getDatasetId(),
                    mergePolicyFactory, mergePolicyFactoryProperties, filterTypeTraits, filterCmpFactories,
                    secondaryBTreeFields, secondaryFilterFields);
            localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(localResourceMetadata,
                    LocalResource.LSMBTreeResource);
            // The index create operation should be persistent regardless of temp datasets or permanent dataset.
            indexDataflowHelperFactory = new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                    dataset.getDatasetId()), mergePolicyFactory, mergePolicyFactoryProperties,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits, filterCmpFactories,
                    secondaryBTreeFields, secondaryFilterFields, true);
        } else {
            // External dataset local resource and dataflow helper
            int[] buddyBreeFields = new int[] { numSecondaryKeys };
            ILocalResourceMetadata localResourceMetadata = new ExternalBTreeWithBuddyLocalResourceMetadata(
                    dataset.getDatasetId(), secondaryComparatorFactories, secondaryTypeTraits, mergePolicyFactory,
                    mergePolicyFactoryProperties, buddyBreeFields);
            localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(localResourceMetadata,
                    LocalResource.ExternalBTreeWithBuddyResource);
            indexDataflowHelperFactory = new ExternalBTreeWithBuddyDataflowHelperFactory(mergePolicyFactory,
                    mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), buddyBreeFields,
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true);
        }
        TreeIndexCreateOperatorDescriptor secondaryIndexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                secondaryFileSplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                secondaryBloomFilterKeyFields, indexDataflowHelperFactory, localResourceFactoryProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(secondaryIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            /*
             * In case of external data, this method is used to build loading jobs for both initial load on index creation
             * and transaction load on dataset referesh
             */
            
            // Create external indexing scan operator
            ExternalDataScanOperatorDescriptor primaryScanOp = createExternalIndexingOp(spec);

            // Assign op.
            AbstractOperatorDescriptor sourceOp = primaryScanOp;
            if (isEnforcingKeyTypes) {
                sourceOp = createCastOp(spec, primaryScanOp, numSecondaryKeys, dataset.getDatasetType());
                spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
            }
            AlgebricksMetaOperatorDescriptor asterixAssignOp = createExternalAssignOp(spec, numSecondaryKeys);

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
            }

            // Tokenizer op
            AbstractOperatorDescriptor tokenizerOp = null;
            if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                tokenizerOp = createTokenizerOp(spec, BuiltinType.ABINARY);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);

            AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
            // Create secondary BTree bulk load op.
            AbstractTreeIndexOperatorDescriptor secondaryBulkLoadOp;
            ExternalBTreeWithBuddyDataflowHelperFactory dataflowHelperFactory = new ExternalBTreeWithBuddyDataflowHelperFactory(
                    mergePolicyFactory, mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(
                            dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE,
                    storageProperties.getBloomFilterFalsePositiveRate(), new int[] { numSecondaryKeys },
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true);
            IOperatorDescriptor root;
            if (externalFiles != null) {
                // Transaction load
                secondaryBulkLoadOp = createExternalIndexBulkModifyOp(spec, numSecondaryKeys, dataflowHelperFactory,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
                root = secondaryBulkLoadOp;
            } else {
                // Initial load
                secondaryBulkLoadOp = createTreeIndexBulkLoadOp(spec, numSecondaryKeys, dataflowHelperFactory,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
                AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                        new IPushRuntimeFactory[] { new SinkRuntimeFactory() },
                        new RecordDescriptor[] { secondaryRecDesc });
                spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
                root = metaOp;
            }
            // Connect the operators.
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, tokenizerOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
                } else {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
                }
            } else {
                if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, tokenizerOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
                } else {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
                }
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.addRoot(root);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        } else {
            // Create dummy key provider for feeding the primary index scan. 
            AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);

            // Create primary index scan op.
            BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

            // Assign op.
            AbstractOperatorDescriptor sourceOp = primaryScanOp;
            if (isEnforcingKeyTypes) {
                sourceOp = createCastOp(spec, primaryScanOp, numSecondaryKeys, dataset.getDatasetType());
                spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
            }
            AlgebricksMetaOperatorDescriptor asterixAssignOp = null;
            if ( indexType == IndexType.STATIC_HILBERT_BTREE) {
                asterixAssignOp = createAssignOpForSHBTree(spec, sourceOp, numSecondaryKeys);
            } else {
                asterixAssignOp = createAssignOp(spec, sourceOp, numSecondaryKeys);
            }

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                selectOp = createFilterNullsSelectOp(spec, numSecondaryKeys);
            }

            // Tokenizer op
            AbstractOperatorDescriptor tokenizerOp = null;
            if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                tokenizerOp = createTokenizerOp(spec, BuiltinType.ABINARY);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);

            AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
            boolean temp = dataset.getDatasetDetails().isTemp();
            // Create secondary BTree bulk load op.
            TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = createTreeIndexBulkLoadOp(
                    spec,
                    numSecondaryKeys,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                            mergePolicyFactory, mergePolicyFactoryProperties,
                            new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), false, filterTypeTraits, filterCmpFactories,
                            secondaryBTreeFields, secondaryFilterFields, !temp), GlobalConfig.DEFAULT_TREE_FILL_FACTOR);

            AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                    new IPushRuntimeFactory[] { new SinkRuntimeFactory() }, new RecordDescriptor[] { secondaryRecDesc });
            // Connect the operators.
            spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, tokenizerOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
                } else {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
                }
            } else {
                if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, tokenizerOp, 0);
                    spec.connect(new OneToOneConnectorDescriptor(spec), tokenizerOp, 0, sortOp, 0);
                } else {
                    spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
                }
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
            spec.addRoot(metaOp);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        }
    }

    private AlgebricksMetaOperatorDescriptor createAssignOpForSHBTree(JobSpecification spec,
            AbstractOperatorDescriptor sourceOp, int numSecondaryKeys) {
        int[] outColumns = new int[numSecondaryKeys + numFilterFields];
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < numSecondaryKeys + numFilterFields; i++) {
            outColumns[i] = numPrimaryKeys + i;
        }
        int projCount = 0;
        for (int i = 0; i < numSecondaryKeys; i++) {
            projectionList[projCount++] = numPrimaryKeys + i;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }
        if (numFilterFields > 0) {
            projectionList[projCount++] = numPrimaryKeys + numSecondaryKeys;
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

    private AbstractOperatorDescriptor createTokenizerOp(JobSpecification spec, IAType tokenType)
            throws AlgebricksException {
        int tokenSourceFieldOffset = 0;
        int tokenFieldsLength = numSecondaryKeys - 1; /* -1 for original point field */
        int nonTokenFieldsLength = numPrimaryKeys + 1 + numFilterFields; /* +1 for original point field */
        int[] nonTokenFields = new int[nonTokenFieldsLength];  
        
        for (int i = 0; i < nonTokenFieldsLength; i++) {
            nonTokenFields[i] = tokenFieldsLength + i;
        }
        IBinaryTokenizerFactory tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(
                tokenType.getTypeTag(), indexType, indexTypeProperty);
        BinaryTokenizerOperatorDescriptor tokenizerOp = new BinaryTokenizerOperatorDescriptor(spec, secondaryRecDesc,
                tokenizerFactory, tokenSourceFieldOffset, nonTokenFields, false, false, 1, false);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, tokenizerOp,
                primaryPartitionConstraint);
        return tokenizerOp;
    }

    @Override
    protected int getNumSecondaryKeys() {
        return numSecondaryKeys;
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        boolean temp = dataset.getDatasetDetails().isTemp();
        LSMTreeIndexCompactOperatorDescriptor compactOp;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider, secondaryTypeTraits,
                    secondaryComparatorFactories, secondaryBloomFilterKeyFields, new LSMBTreeDataflowHelperFactory(
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                            mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE,
                            storageProperties.getBloomFilterFalsePositiveRate(), false, filterTypeTraits,
                            filterCmpFactories, secondaryBTreeFields, secondaryFilterFields, !temp),
                    NoOpOperationCallbackFactory.INSTANCE);
        } else {
            // External dataset
            compactOp = new LSMTreeIndexCompactOperatorDescriptor(spec,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider, secondaryTypeTraits,
                    secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                    new ExternalBTreeWithBuddyDataflowHelperFactory(mergePolicyFactory, mergePolicyFactoryProperties,
                            new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), new int[] { numSecondaryKeys },
                            ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true),
                    NoOpOperationCallbackFactory.INSTANCE);
        }
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);
        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void setSecondaryRecDescAndComparators(IndexTypeProperty indexTypeProperty, List<List<String>> secondaryKeyFields,
    		List<IAType> secondaryKeyTypes, AqlMetadataProvider metadataProvider) throws AlgebricksException, AsterixException {
        this.indexTypeProperty = indexTypeProperty;
        int bloomfilterKeyFieldsCount = numSecondaryKeys;

        if (indexType == IndexType.DYNAMIC_HILBERTVALUE_BTREE || indexType == IndexType.STATIC_HILBERT_BTREE) {
            /*
             * DYNAMIC_HILBERTVALUE_BTREE index has the following fields in an entry.
             * [ Hilbert value (AINT64) | point (APOINT) | PK ]
             * STATIC_HILBERT_BTREE index has the following fields in an entry.
             * [ Cell number (ABINARY) | point (APOINT) | PK ]
             */
            ++numSecondaryKeys;
        }

        secondaryFieldAccessEvalFactories = new ICopyEvaluatorFactory[numSecondaryKeys + numFilterFields];
        secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        secondaryBloomFilterKeyFields = new int[bloomfilterKeyFieldsCount];
        
        ITypeTraits[] assignOpTypeTraits = null;
        ISerializerDeserializer[] assignOpRecFieldsSerde = null;
        if (indexType == IndexType.STATIC_HILBERT_BTREE) {
            assignOpTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys + numFilterFields];
            assignOpRecFieldsSerde = new ISerializerDeserializer[numSecondaryKeys + numPrimaryKeys + numFilterFields];
        }

        ISerializerDeserializer[] secondaryRecFieldsSerde = new ISerializerDeserializer[numSecondaryKeys
                + numPrimaryKeys + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializer[] enforcedRecFields = new ISerializerDeserializer[1 + numPrimaryKeys + numFilterFields];
        ITypeTraits[] enforcedTypeTraits = new ITypeTraits[1 + numPrimaryKeys];
        
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getFormat().getTypeTraitProvider();
        IBinaryComparatorFactoryProvider comparatorFactoryProvider = metadataProvider.getFormat()
                .getBinaryComparatorFactoryProvider();
        // Record column is 0 for external datasets, numPrimaryKeys for internal ones
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;

        if (indexType == IndexType.DYNAMIC_HILBERTVALUE_BTREE) {
            //eval factory for Hilbert value generation
            List<String> secondaryKeyFieldName = secondaryKeyFields.get(0);
            
            ICopyEvaluatorFactory pointEvalFactory = metadataProvider.getFormat().getFieldAccessEvaluatorFactory(
            		isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFieldName, recordColumn);
            secondaryFieldAccessEvalFactories[0] = new ComputeInt64HilbertValueEvalFactory(pointEvalFactory);
            secondaryRecFieldsSerde[0] = serdeProvider.getSerializerDeserializer(BuiltinType.AINT64);
            secondaryComparatorFactories[0] = comparatorFactoryProvider.getBinaryComparatorFactory(BuiltinType.AINT64,
                    true);
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(BuiltinType.AINT64);
            secondaryBloomFilterKeyFields[0] = 0;

            //eval factory for point generation
            secondaryFieldAccessEvalFactories[1] = metadataProvider.getFormat().getFieldAccessEvaluatorFactory(
            		isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFieldName, recordColumn);
            secondaryRecFieldsSerde[1] = serdeProvider.getSerializerDeserializer(BuiltinType.APOINT);
            secondaryComparatorFactories[1] = comparatorFactoryProvider.getBinaryComparatorFactory(BuiltinType.APOINT,
                    true);
            secondaryTypeTraits[1] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);

            //set nullable flag
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(BuiltinType.APOINT, secondaryKeyFieldName, itemType);
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
        } else if (indexType == IndexType.STATIC_HILBERT_BTREE) {

            List<String> secondaryKeyFieldName = secondaryKeyFields.get(0);
            
            //#. for assignOp: input [PK,REC] --> output [APOINT,APOINT,PK]
            //the first point is fed to cell tokenizer, and the second one is used to store original point.
            //eval factories for point generation 
            secondaryFieldAccessEvalFactories[0] = metadataProvider.getFormat().getFieldAccessEvaluatorFactory(
                    isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFieldName, recordColumn);
            secondaryFieldAccessEvalFactories[1] = metadataProvider.getFormat().getFieldAccessEvaluatorFactory(
                    isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFieldName, recordColumn);
            //for assignOpRec descriptor
            assignOpRecFieldsSerde[0] = serdeProvider.getSerializerDeserializer(BuiltinType.APOINT);
            assignOpTypeTraits[0] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);
            assignOpRecFieldsSerde[1] = serdeProvider.getSerializerDeserializer(BuiltinType.APOINT);
            assignOpTypeTraits[1] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);
            
            //#. for secondaryIndexOp: input [Cell number(ABINARY), point(APOINT), PK]
            secondaryRecFieldsSerde[0] = serdeProvider.getSerializerDeserializer(BuiltinType.ABINARY);
            secondaryComparatorFactories[0] = comparatorFactoryProvider.getBinaryComparatorFactory(BuiltinType.ABINARY,
                    true);
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(BuiltinType.ABINARY);
            secondaryBloomFilterKeyFields[0] = 0;
            secondaryRecFieldsSerde[1] = serdeProvider.getSerializerDeserializer(BuiltinType.APOINT);
            secondaryComparatorFactories[1] = comparatorFactoryProvider.getBinaryComparatorFactory(BuiltinType.APOINT,
                    true);
            secondaryTypeTraits[1] = typeTraitProvider.getTypeTrait(BuiltinType.APOINT);

            //set nullable flag
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(BuiltinType.APOINT, secondaryKeyFieldName, itemType);
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
        } else {
            for (int i = 0; i < numSecondaryKeys; i++) {
                secondaryFieldAccessEvalFactories[i] = metadataProvider.getFormat().getFieldAccessEvaluatorFactory(
                		isEnforcingKeyTypes ? enforcedItemType : itemType, secondaryKeyFields.get(i), recordColumn);
                Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(i),
                        secondaryKeyFields.get(i), itemType);
                IAType keyType = keyTypePair.first;
                anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;

                secondaryRecFieldsSerde[i] = serdeProvider.getSerializerDeserializer(keyType);
                secondaryComparatorFactories[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
                secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(keyType);
                secondaryBloomFilterKeyFields[i] = i;
            }
        }

        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            // Add serializers and comparators for primary index fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFieldsSerde[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
                enforcedRecFields[i] = primaryRecDesc.getFields()[i];
                secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
                enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
                secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
                if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                    assignOpRecFieldsSerde[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
                    assignOpTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
                }
            }
        } else {
            // Add serializers and comparators for RID fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFieldsSerde[numSecondaryKeys + i] = IndexingConstants.getSerializerDeserializer(i);
                enforcedRecFields[i] = IndexingConstants.getSerializerDeserializer(i);
                secondaryTypeTraits[numSecondaryKeys + i] = IndexingConstants.getTypeTraits(i);
                enforcedTypeTraits[i] = IndexingConstants.getTypeTraits(i);
                secondaryComparatorFactories[numSecondaryKeys + i] = IndexingConstants.getComparatorFactory(i);
                if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                    assignOpRecFieldsSerde[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
                    assignOpTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
                }
            }
        }
        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        
        if (numFilterFields > 0) {
            secondaryFieldAccessEvalFactories[numSecondaryKeys] = metadataProvider.getFormat()
                    .getFieldAccessEvaluatorFactory(itemType, filterFieldName, recordColumn);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(filterFieldName, itemType);
            IAType type = keyTypePair.first;
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(type);
            secondaryRecFieldsSerde[numPrimaryKeys + numSecondaryKeys] = serde;
            if (indexType == IndexType.STATIC_HILBERT_BTREE) {
                assignOpRecFieldsSerde[numPrimaryKeys + numSecondaryKeys] = serde;
            }
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFieldsSerde, secondaryTypeTraits);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);
        if (indexType == IndexType.STATIC_HILBERT_BTREE) {
            assignOpRecDesc = new RecordDescriptor(assignOpRecFieldsSerde, assignOpTypeTraits);
        }
    }

}
