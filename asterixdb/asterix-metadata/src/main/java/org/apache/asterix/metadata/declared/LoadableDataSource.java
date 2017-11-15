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
package org.apache.asterix.metadata.declared;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang.StringUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;

public class LoadableDataSource extends DataSource {

    private final Dataset targetDataset;
    private final List<List<String>> partitioningKeys;
    private final String adapter;
    private final Map<String, String> adapterProperties;
    private final boolean isPKAutoGenerated;

    public LoadableDataSource(Dataset targetDataset, IAType itemType, IAType metaItemType, String adapter,
            Map<String, String> properties) throws AlgebricksException, IOException {
        super(new DataSourceId("loadable_dv", "loadable_ds"), itemType, metaItemType, Type.LOADABLE, null);
        this.targetDataset = targetDataset;
        this.adapter = adapter;
        this.adapterProperties = properties;
        partitioningKeys = targetDataset.getPrimaryKeys();
        ARecordType recType = (ARecordType) itemType;
        isPKAutoGenerated = ((InternalDatasetDetails) targetDataset.getDatasetDetails()).isAutogenerated();
        if (isPKAutoGenerated) {
            // Since the key is auto-generated, we need to use another
            // record type (possibly nested) which has all fields except the PK
            recType = getStrippedPKType(new LinkedList<>(partitioningKeys.get(0)), recType);
        }
        schemaTypes = new IAType[] { recType };
    }

    private ARecordType getStrippedPKType(List<String> partitioningKeys, ARecordType recType)
            throws AlgebricksException, HyracksDataException {
        List<String> fieldNames = new LinkedList<>();
        List<IAType> fieldTypes = new LinkedList<>();
        int j = 0;
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            IAType fieldType;
            if (partitioningKeys.get(0).equals(recType.getFieldNames()[j])) {
                if (recType.getFieldTypes()[j].getTypeTag() == ATypeTag.OBJECT) {
                    if (j != 0) {
                        throw new AsterixException("Autogenerated key " + StringUtils.join(partitioningKeys, '.')
                                + " should be a first field of the type " + recType.getTypeName());
                    }
                    partitioningKeys.remove(0);
                    fieldType = getStrippedPKType(partitioningKeys, (ARecordType) recType.getFieldTypes()[j]);
                } else {
                    j++;
                    continue;
                }
            } else {
                fieldType = recType.getFieldTypes()[j];
            }
            fieldTypes.add(fieldType);
            fieldNames.add(recType.getFieldNames()[j]);
            j++;
        }
        return new ARecordType(recType.getTypeName(), fieldNames.toArray(new String[0]),
                fieldTypes.toArray(new IAType[0]), recType.isOpen());
    }

    public List<List<String>> getPartitioningKeys() {
        return partitioningKeys;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getAdapterProperties() {
        return adapterProperties;
    }

    public IAType getLoadedType() {
        return schemaTypes[schemaTypes.length - 1];
    }

    public Dataset getTargetDataset() {
        return targetDataset;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException {
        LoadableDataSource alds = (LoadableDataSource) dataSource;
        ARecordType itemType = (ARecordType) alds.getLoadedType();
        IAdapterFactory adapterFactory = metadataProvider.getConfiguredAdapterFactory(alds.getTargetDataset(),
                alds.getAdapter(), alds.getAdapterProperties(), itemType, null);
        RecordDescriptor rDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
        return metadataProvider.buildLoadableDatasetScan(jobSpec, adapterFactory, rDesc);
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return true;
    }
}
