package org.apache.asterix.external.ipc;

public enum MessageType {
    HELO((byte) 0),
    QUIT((byte) 1),
    INIT((byte) 2),
    INIT_RSP((byte) 3),
    CALL((byte) 4),
    CALL_RSP((byte) 5);

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
