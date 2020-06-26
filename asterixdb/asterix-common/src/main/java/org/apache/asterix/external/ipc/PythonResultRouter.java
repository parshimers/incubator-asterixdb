package org.apache.asterix.external.ipc;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.Message;

public class PythonResultRouter implements IIPCI {

    ConcurrentHashMap<Quadruple<Long, Integer, Integer, Integer>, ByteBuffer> activeClients = new ConcurrentHashMap<>();
    Quadruple<Long, Integer, Integer, Integer> mutableKey = new Quadruple<>(-1l, -1, -1, -1);

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
        ByteBuffer buf = (ByteBuffer) payload;
        //long tag b
        buf.position(Message.HEADER_SIZE+4);
        mutableKey.setFirst(buf.getLong());
        mutableKey.setSecond(buf.getInt());
        mutableKey.setThird(buf.getInt());
        mutableKey.setFourth(buf.getInt());
        ByteBuffer copyTo = activeClients.get(mutableKey);
        assert copyTo != null; //TODO: REMOVE. TESTING.
        for(int i = buf.position();i<buf.limit();i++){
            copyTo.put(buf.get());
        }
        copyTo.limit(copyTo.position()+1);
        copyTo.position(0);
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

    public void insertRoute(Quadruple<Long, Integer, Integer, Integer> route, ByteBuffer respBuffer) {
        activeClients.put(route, respBuffer);
    }

    public static class NoOpNoSerJustDe implements IPayloadSerializerDeserializer {

        @Override
        public Object deserializeObject(ByteBuffer buffer, int length) throws Exception {
            //TODO: dont assert
            assert length > 10;
            return buffer;
        }

        @Override
        public Exception deserializeException(ByteBuffer buffer, int length) throws Exception {
            return null;
        }

        @Override
        public byte[] serializeObject(Object object) throws Exception {
            if(object != null) {
                return ((ByteBuffer) object).array();
            }
            else return new byte[1];
        }

        @Override
        public byte[] serializeException(Exception object) throws Exception {
            throw new UnsupportedOperationException("nice try");
        }
    }
}
