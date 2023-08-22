package org.apache.hyracks.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface HyracksRunnable {
    void run() throws HyracksDataException;
}
