package org.apache.hyracks.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface HyracksCallable<V> {
    V call() throws HyracksDataException;
}
