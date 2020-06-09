package org.apache.asterix.external.ipc;

public enum MessageType {
    HELO((byte) 1),
    QUIT((byte) 2),
    INIT((byte) 3),
    INIT_RSP((byte) 4),
    CALL((byte) 5),
    CALL_RSP((byte) 6);

    private final byte msg;

    static final MessageType[] messageTypes = new MessageType[6];
    static {
        for (MessageType m : values()) {
            messageTypes[m.getValue()] = m;
        }
    }

    MessageType(byte b) {
        this.msg = b;
    }

    public byte getValue() {
        return msg;
    }

    public static MessageType fromByte(byte b) {
        return messageTypes[b];
    }
}
