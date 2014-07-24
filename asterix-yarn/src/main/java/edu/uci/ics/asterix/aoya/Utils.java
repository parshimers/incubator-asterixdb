package edu.uci.ics.asterix.aoya;

import java.util.Random;

import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;

public class Utils {

    public static String hostFromContainerID(String containerID) {
        return containerID.split("_")[4];
    }

    public static Node getMetadataNode(Cluster cluster) {
        Node metadataNode = null;
        if (cluster.getMetadataNode() != null) {
            for (Node node : cluster.getNode()) {
                if (node.getId().equals(cluster.getMetadataNode())) {
                    metadataNode = node;
                    break;
                }
            }
        } else {
            //I will pick one for you.
            metadataNode = cluster.getNode().get(1);
        }
        return metadataNode;
    }

}
