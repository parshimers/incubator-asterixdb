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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.connectors.MToOneSequentialMergingConnectorDescriptor;

public class SequentialMergeExchangePOperator extends AbstractExchangePOperator {
    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SEQUENTIAL_MERGE_EXCHANGE;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        List<ILocalStructuralProperty> childLocalProps = childOp.getDeliveredPhysicalProperties().getLocalProperties();
        List<ILocalStructuralProperty> localProperties;
        if (childLocalProps != null) {
            localProperties = new ArrayList<>(childLocalProps);
        } else {
            localProperties = new ArrayList<>(0);
        }

        deliveredProperties = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, localProperties);
    }

    @Override
    public Pair<IConnectorDescriptor, IHyracksJobBuilder.TargetConstraint> createConnectorDescriptor(
            IConnectorDescriptorRegistry spec, ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        IConnectorDescriptor connector = new MToOneSequentialMergingConnectorDescriptor(spec);
        return new Pair<>(connector, IHyracksJobBuilder.TargetConstraint.ONE);
    }
}
