package org.apache.asterix.external.ipc;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.Message;

public class PythonResultRouter implements IIPCI {

    AtomicLong minId = new AtomicLong(0);
    AtomicLong maxId = new AtomicLong(0);
    ConcurrentHashMap<Long, Pair<Exchanger<ByteBuffer>, ByteBuffer>> activeClients = new ConcurrentHashMap<>();

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
        int rewind = handle.getAttachmentLen();
        ByteBuffer buf = (ByteBuffer) payload;
        //long tag b
        int end = buf.position();
        buf.position(end-rewind);
        Pair<Exchanger<ByteBuffer>, ByteBuffer> route = activeClients.get(rmid);
        ByteBuffer copyTo = route.second;
        assert copyTo != null; //TODO: REMOVE. TESTING.
        copyTo.position(0);
        for (int i = 0; i <handle.getAttachmentLen(); i++) {
            copyTo.put(buf.get());
        }
        copyTo.flip();
        try {
            ByteBuffer fresh = route.first.exchange(copyTo);
            route.second = fresh;
        } catch (InterruptedException e) {
            //? duno
        }
        buf.position(end);

    }

    @Override
    public void onError(IIPCHandle handle, long mid, long rmid, Exception exception) {
        //TODO: important.
    }

    public Long insertRoute(ByteBuffer buf, Exchanger<ByteBuffer> exch) {
        long id = maxId.incrementAndGet();
        activeClients.put(id, new Pair<>(exch, buf));
        return id;
    }

    public ByteBuffer getBuf(Long id) {
        return activeClients.get(id).second;
    }

    public void removeRoute(Long id) {
        // ???
        if (id <= minId.get()) {
            minId.decrementAndGet();
        }
        activeClients.remove(id);
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
            return new byte[1];
        }

        @Override
        public byte[] serializeException(Exception object) throws Exception {
            throw new UnsupportedOperationException("nice try");
        }

        @Override
        public Object deserializeControlObject(ByteBuffer buffer, int length) throws Exception {
            //TODO: ugh
            return new JavaSerializationBasedPayloadSerializerDeserializer().deserializeControlObject(buffer, length);
        }
    }
}
