package edu.uci.ics.asterix.common.feeds;

public interface IFeedMessageService {

    
    public void start() throws Exception;
    
    public void stop();
    
    public void sendMessage(IFeedMessage message);
}
