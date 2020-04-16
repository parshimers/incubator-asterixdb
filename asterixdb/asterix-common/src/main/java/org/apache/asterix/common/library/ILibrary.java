package org.apache.asterix.common.library;

public interface ILibrary<T> {

    T get();

    void close();
}
