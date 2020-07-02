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

import javax.naming.InsufficientResourcesException;

public class PythonResultRouter implements IIPCI {

    AtomicLong minId = new AtomicLong(0);
    AtomicLong maxId = new AtomicLong(0);
    ConcurrentHashMap<Long, Pair<Exchanger<ByteBuffer>, ByteBuffer>> activeClients = new ConcurrentHashMap<>();
    private static int MAX_BUF_SIZE = 21 * 1024 * 1024; //21MB

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
        int rewind = handle.getAttachmentLen();
        ByteBuffer buf = (ByteBuffer) payload;
        int end = buf.position();
        buf.position(end-rewind);
        Pair<Exchanger<ByteBuffer>, ByteBuffer> route = activeClients.get(rmid);
        ByteBuffer copyTo = route.second;
        if(copyTo.capacity() < handle.getAttachmentLen()){
            int nextSize = closestPow2(handle.getAttachmentLen());
            if(nextSize > MAX_BUF_SIZE){
                //TODO: something more graceful
                throw new IllegalArgumentException("Message too big");
            }
            copyTo = ByteBuffer.allocate(nextSize);
        }
        copyTo.position(0);
        System.arraycopy(buf.array(),buf.position()+buf.arrayOffset(),copyTo.array(),copyTo.arrayOffset(),handle.getAttachmentLen());
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

    //TODO: make this so you can't overflow the id
    public void removeRoute(Long id) {
        if (id <= minId.get()) {
            minId.decrementAndGet();
        }
        activeClients.remove(id);
    }

    public static int closestPow2(int n){
        return (int) Math.pow(2,Math.ceil(Math.log(n)/Math.log(2)));
    }

    public static class NoOpNoSerJustDe implements IPayloadSerializerDeserializer {

        private static byte[] noop = new byte[]{(byte)0};

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
            return noop;
        }

        @Override
        public byte[] serializeException(Exception object) throws Exception {
            return noop;
        }

        @Override
        public Object deserializeControlObject(ByteBuffer buffer, int length) throws Exception {
            return new JavaSerializationBasedPayloadSerializerDeserializer().deserializeControlObject(buffer, length);
        }
    }
}
