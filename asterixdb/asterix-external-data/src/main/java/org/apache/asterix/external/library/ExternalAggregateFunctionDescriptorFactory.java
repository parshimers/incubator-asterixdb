package org.apache.asterix.external.library;

import java.io.Serializable;

import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;

public class ExternalAggregateFunctionDescriptorFactory implements IFunctionDescriptorFactory, Serializable {

    private final IExternalFunctionInfo fInfo;

    public ExternalAggregateFunctionDescriptorFactory(IExternalFunctionInfo fInfo) {
        this.fInfo = fInfo;
    }

    @Override
    public IFunctionDescriptor createFunctionDescriptor() {
        return new ExternalAggregateFunctionDescriptor(fInfo);
    }

    public IExternalFunctionInfo getfInfo() {
        return fInfo;
    }
}
