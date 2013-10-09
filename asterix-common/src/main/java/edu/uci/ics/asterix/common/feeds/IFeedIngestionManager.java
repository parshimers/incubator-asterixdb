package edu.uci.ics.asterix.common.feeds;

public interface IFeedIngestionManager {

    public void registerFeedIngestionRuntime(IngestionRuntime ingestionRuntime);

    public void deregisterFeedIngestionRuntime(FeedIngestionId ingestionId);

    public IngestionRuntime getIngestionRuntime(FeedIngestionId feedIngestionId);
}
