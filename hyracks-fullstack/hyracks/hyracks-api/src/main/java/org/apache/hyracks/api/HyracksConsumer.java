package org.apache.hyracks.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

@FunctionalInterface
public interface HyracksConsumer<T> {
    void accept(final T elem) throws HyracksDataException;
}
