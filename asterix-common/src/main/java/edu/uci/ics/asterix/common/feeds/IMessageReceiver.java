package edu.uci.ics.asterix.common.feeds;

public interface IMessageReceiver<T> {

    public void sendMessage(T message);

    public void close(boolean processPending);

    void start();
}
