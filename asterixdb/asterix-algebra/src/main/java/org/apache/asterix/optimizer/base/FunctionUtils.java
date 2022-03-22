package org.apache.asterix.optimizer.base;

import java.util.List;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtils {

    public static FunctionIdentifier getBuiltinAggOrUDF(AbstractFunctionCallExpression fc, MetadataProvider mdPv)
            throws AlgebricksException {
        //TODO: some way to avoid this FunctionSignature dance?
        FunctionIdentifier fid = fc.getFunctionIdentifier();
        FunctionSignature fSig =
                new FunctionSignature(FunctionSignature.getDataverseName(fid), "agg-" + fid.getName(), fid.getArity());
        org.apache.asterix.metadata.entities.Function udf = mdPv.lookupUserDefinedFunction(fSig);
        if (udf != null) {
            return udf.getSignature().createFunctionIdentifier();
        } else
            return BuiltinFunctions.getAggregateFunction(fc.getFunctionIdentifier());
    }

    public static AggregateFunctionCallExpression getBuiltinAggExprOrUDF(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args, MetadataProvider mdPv) throws AlgebricksException {
        FunctionSignature fSig = new FunctionSignature(fi);
        org.apache.asterix.metadata.entities.Function udf = mdPv.lookupUserDefinedFunction(fSig);
        if (udf != null) {
            IFunctionInfo finfo = ExternalFunctionCompilerUtil.getExternalFunctionInfo(mdPv, udf);
            AggregateFunctionCallExpression expr = new AggregateFunctionCallExpression(finfo, true, args);
            expr.setStepOneAggregate(finfo);
            expr.setStepTwoAggregate(finfo);
            return expr;
        } else
            return BuiltinFunctions.makeAggregateFunctionExpression(fi, args);
    }
}
