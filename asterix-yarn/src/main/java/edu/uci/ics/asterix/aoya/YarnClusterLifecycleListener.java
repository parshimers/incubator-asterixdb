package edu.uci.ics.asterix.aoya;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.uci.ics.hyracks.api.application.IClusterLifecycleListener;

public class YarnClusterLifecycleListener implements IClusterLifecycleListener {
    Logger LOG = Logger.getLogger(YarnClusterLifecycleListener.class);
    ApplicationMaster am;

    public YarnClusterLifecycleListener(ApplicationMaster am) {
        this.am = am;
    }

    @Override
    public void notifyNodeJoin(String nodeId, Map<String, String> ncConfiguration) {
        //not sure if this info really helps the AM
    }

    @Override
    public void notifyNodeFailure(Set<String> deadNodeIds) {
        for (String node : deadNodeIds) {
            try {
                am.notifyDeadNode(node);
            } catch (UnknownHostException e) {
                LOG.error("AM notified of dead NC that does not resolve to a hostname?");
            }
        }
    }

}
