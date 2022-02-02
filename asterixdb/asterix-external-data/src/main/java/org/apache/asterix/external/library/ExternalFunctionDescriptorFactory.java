package org.apache.asterix.external.library;

import org.apache.asterix.om.functions.ExternalFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ExternalFunctionDescriptorFactory implements IFunctionDescriptorFactory {

    private final IExternalFunctionInfo fInfo;

    public ExternalFunctionDescriptorFactory(ExternalFunctionInfo fInfo) {
        this.fInfo = fInfo;
    }

    @Override
    public IFunctionDescriptor createFunctionDescriptor() {
        try {
            return ExternalFunctionDescriptorProvider.getExternalFunctionDescriptor(fInfo);
        } catch (AlgebricksException e) {
            e.printStackTrace();
        }
        return null;
    }

    public IExternalFunctionInfo getfInfo() {
        return fInfo;
    }
}
