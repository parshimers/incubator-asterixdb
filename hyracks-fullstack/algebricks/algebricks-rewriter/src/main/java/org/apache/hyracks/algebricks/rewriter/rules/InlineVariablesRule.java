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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Replaces variable reference expressions with their assigned function-call expression where applicable
 * (some variables are generated by datasources).
 * Inlining variables may enable other optimizations by allowing selects and assigns to be moved
 * (e.g., a select may be pushed into a join to enable an efficient physical join operator).
 * Preconditions/Assumptions:
 * Assumes no projects are in the plan. Only inlines variables whose assigned expression is a function call
 * (i.e., this rule ignores right-hand side constants and other variable references expressions
 * Postconditions/Examples:
 * All qualifying variables have been inlined.
 * Example (simplified):
 * Before plan:
 * select <- [$$1 < $$2 + $$0]
 * assign [$$2] <- [funcZ() + $$0]
 * assign [$$0, $$1] <- [funcX(), funcY()]
 * After plan:
 * select <- [funcY() < funcZ() + funcX() + funcX()]
 * assign [$$2] <- [funcZ() + funcX()]
 * assign [$$0, $$1] <- [funcX(), funcY()]
 */
public class InlineVariablesRule implements IAlgebraicRewriteRule {

    // Map of variables that could be replaced by their producing expression.
    // Populated during the top-down sweep of the plan.
    protected Map<LogicalVariable, ILogicalExpression> varAssignRhs = new HashMap<>();
    // Visitor for replacing variable reference expressions with their originating expression.
    protected InlineVariablesVisitor inlineVisitor = new InlineVariablesVisitor(varAssignRhs);
    // Set of FunctionIdentifiers that we should not inline.
    protected Set<FunctionIdentifier> doNotInlineFuncs = new HashSet<>();
    // Indicates whether the rule has been run
    protected boolean hasRun = false;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (hasRun) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        prepare(context);
        boolean modified = inlineVariables(opRef, context);
        if (performFinalAction()) {
            modified = true;
        }
        hasRun = true;
        return modified;
    }

    protected void prepare(IOptimizationContext context) {
        varAssignRhs.clear();
        inlineVisitor.setContext(context);
    }

    protected boolean performBottomUpAction(AbstractLogicalOperator op) throws AlgebricksException {
        // Only inline variables in operators that can deal with arbitrary expressions.
        if (!op.requiresVariableReferenceExpressions()) {
            inlineVisitor.setOperator(op);
            return op.acceptExpressionTransform(inlineVisitor);
        }
        return false;
    }

    protected boolean performFinalAction() throws AlgebricksException {
        return false;
    }

    protected boolean inlineVariables(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Update mapping from variables to expressions during top-down traversal.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            List<LogicalVariable> vars = assignOp.getVariables();
            List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();
            for (int i = 0; i < vars.size(); i++) {
                ILogicalExpression expr = exprs.get(i).getValue();
                // Ignore functions that are either in the doNotInline set or are non-functional
                if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                    if (doNotInlineFuncs.contains(funcExpr.getFunctionIdentifier()) || !funcExpr.isFunctional()) {
                        continue;
                    }
                }
                varAssignRhs.put(vars.get(i), exprs.get(i).getValue());
            }
        }

        // Descend into children removing projects on the way.
        boolean modified = false;
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (inlineVariables(inputOpRef, context)) {
                modified = true;
            }
        }

        // Descend into subplan
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            SubplanOperator subplanOp = (SubplanOperator) op;
            for (ILogicalPlan nestedPlan : subplanOp.getNestedPlans()) {
                for (Mutable<ILogicalOperator> root : nestedPlan.getRoots()) {
                    if (inlineVariables(root, context)) {
                        modified = true;
                    }
                    // Variables produced by a nested subplan cannot be inlined
                    // in operators above the subplan.
                    Set<LogicalVariable> producedVars = new HashSet<>();
                    VariableUtilities.getProducedVariables(root.getValue(), producedVars);
                    varAssignRhs.keySet().removeAll(producedVars);
                }
            }
        }

        // References to variables generated in the right branch of a left-outer-join cannot be inlined
        // in operators above the left-outer-join.
        if (op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            Set<LogicalVariable> rightLiveVars = new HashSet<>();
            VariableUtilities.getLiveVariables(op.getInputs().get(1).getValue(), rightLiveVars);
            varAssignRhs.keySet().removeAll(rightLiveVars);
        }

        if (performBottomUpAction(op)) {
            modified = true;
        }

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            context.addToDontApplySet(this, op);
            // Re-enable rules that we may have already tried. They could be applicable now after inlining.
            context.removeFromAlreadyCompared(opRef.getValue());
        }

        return modified;
    }

    public static class InlineVariablesVisitor implements ILogicalExpressionReferenceTransform {

        private final Map<LogicalVariable, ILogicalExpression> varAssignRhs;
        private final Set<LogicalVariable> liveVars = new HashSet<>();
        private final List<LogicalVariable> rhsUsedVars = new ArrayList<>();
        private ILogicalOperator op;
        private IOptimizationContext context;
        // If set, only replace this variable reference.
        private LogicalVariable targetVar;

        public InlineVariablesVisitor(Map<LogicalVariable, ILogicalExpression> varAssignRhs) {
            this.varAssignRhs = varAssignRhs;
        }

        public void setTargetVariable(LogicalVariable targetVar) {
            this.targetVar = targetVar;
        }

        public void setContext(IOptimizationContext context) {
            this.context = context;
        }

        public void setOperator(ILogicalOperator op) throws AlgebricksException {
            this.op = op;
            liveVars.clear();
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression e = exprRef.getValue();
            switch (((AbstractLogicalExpression) e).getExpressionTag()) {
                case VARIABLE:
                    return transformVariableReferenceExpression(exprRef,
                            ((VariableReferenceExpression) e).getVariableReference());
                case FUNCTION_CALL:
                    return transformFunctionCallExpression((AbstractFunctionCallExpression) e);
                default:
                    return false;
            }
        }

        private boolean transformFunctionCallExpression(AbstractFunctionCallExpression fce) throws AlgebricksException {
            boolean modified = false;
            for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                if (transform(arg)) {
                    modified = true;
                }
            }
            return modified;
        }

        private boolean transformVariableReferenceExpression(Mutable<ILogicalExpression> exprRef, LogicalVariable var)
                throws AlgebricksException {
            // Restrict replacement to targetVar if it has been set.
            if (targetVar != null && var != targetVar) {
                return false;
            }

            // Make sure has not been excluded from inlining.
            if (context.shouldNotBeInlined(var)) {
                return false;
            }

            ILogicalExpression rhs = varAssignRhs.get(var);
            if (rhs == null) {
                // Variable was not produced by an assign.
                return false;
            }

            // Make sure used variables from rhs are live.
            if (liveVars.isEmpty()) {
                VariableUtilities.getLiveVariables(op, liveVars);
            }
            rhsUsedVars.clear();
            rhs.getUsedVariables(rhsUsedVars);
            for (LogicalVariable rhsUsedVar : rhsUsedVars) {
                if (!liveVars.contains(rhsUsedVar)) {
                    return false;
                }
            }

            // Replace variable reference with a clone of the rhs expr.
            exprRef.setValue(rhs.cloneExpression());
            return true;
        }
    }
}
