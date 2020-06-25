package org.apache.asterix.external.ipc;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class PythonResultRouter implements IIPCI {


    ConcurrentHashMap<Pair<Integer, Integer>, ByteBuffer> activeClients = new ConcurrentHashMap<>();
    MutablePair<Integer, Integer> key;

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
        //todo: idk? seems wrong
        int pid = (int) rmid >> 32;
        int fid = (int) rmid;
        key.setLeft(pid);
        key.setRight(fid);
        ByteBuffer copyTo = activeClients.get(key);
        assert copyTo != null; // REMOVE. TESTING.
        ByteBuffer copyFrom = (ByteBuffer) payload;
        System.arraycopy(payload,
                ((ByteBuffer) payload).position() + ((ByteBuffer) payload).arrayOffset(),
                copyTo, copyTo.arrayOffset() + copyTo.position(), copyFrom.limit());
        //todo: it seems improbable, but if the result were to arrive before the thread that
        //      triggered this had started waiting, or it was interrupted, we would lose the result
        synchronized (copyTo) {
            copyTo.notify();
        }
    }

    @Override
    public void onError(IIPCHandle handle, long mid, long rmid, Exception exception) {
        //TODO: important.
    }

    public void insertRoute(int pid, int fnId, ByteBuffer respBuffer) {
        activeClients.put(new ImmutablePair(pid, fnId), respBuffer);
    }


    public static class NoOpNoSerJustDe implements IPayloadSerializerDeserializer {

        @Override
        public Object deserializeObject(ByteBuffer buffer, int length) throws Exception {
            return buffer;
        }

        @Override
        public Exception deserializeException(ByteBuffer buffer, int length) throws Exception {
            return null;
        }

        @Override
        public byte[] serializeObject(Object object) throws Exception {
            throw new UnsupportedOperationException("nice try");
        }

        @Override
        public byte[] serializeException(Exception object) throws Exception {
            throw new UnsupportedOperationException("nice try");
        }
    }
}
