package org.apache.hyracks.storage.am.common.impls;

import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

public class ExtendedIndexAccessParameters extends IndexAccessParameters implements IIndexAccessParameters {

    protected final IExtendedModificationOperationCallback extendedModificationCallback;
    // This map is used to put additional parameters to an index accessor.

    public ExtendedIndexAccessParameters(IExtendedModificationOperationCallback extendedModificationCallback,
            ISearchOperationCallback searchOperationCallback) {
        super(extendedModificationCallback, searchOperationCallback);
        this.extendedModificationCallback = extendedModificationCallback;
    }

    @Override
    public IExtendedModificationOperationCallback getModificationCallback() {
        return extendedModificationCallback;
    }
}
