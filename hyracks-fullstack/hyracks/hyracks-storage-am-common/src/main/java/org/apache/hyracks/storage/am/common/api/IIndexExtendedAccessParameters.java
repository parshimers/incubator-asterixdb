package org.apache.hyracks.storage.am.common.api;

import org.apache.hyracks.storage.common.IIndexAccessParameters;

public interface IIndexExtendedAccessParameters extends IIndexAccessParameters {

    @Override
    IExtendedModificationOperationCallback getModificationCallback();
}
