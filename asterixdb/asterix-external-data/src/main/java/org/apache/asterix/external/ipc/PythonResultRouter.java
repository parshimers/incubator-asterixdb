package org.apache.asterix.external.ipc;

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PythonResultRouter implements IIPCI {

    class PythonConditionHolder {
        Lock lock;
        Condition sent;
    }

    ConcurrentHashMap<Pair<Integer,Integer>, Lock> waitingResponses = new ConcurrentHashMap<>();
    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
    }

    @Override
    public void onError(IIPCHandle handle, long mid, long rmid, Exception exception) {

    }
    public void waitForFunctionResult(int pid, int fnId, ByteBuffer respBuffer, Lock bufferLock, Condition full){
        //wait for buffer to fill
        synchronized (respBuffer) {
            respBuffer.
        }
    }
}
