package org.apache.asterix.common.library;

public interface ILibrary<T> {

    void init();

    T get();

    void set (T artifact);

    void close();
}
