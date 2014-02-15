package edu.uci.ics.asterix.experiment.action.derived;

public class RemoteAsterixDriverKill extends AbstractRemoteExecutableAction {

    public RemoteAsterixDriverKill(String hostname, String username, String keyLocation) {
        super(hostname, username, keyLocation);
    }

    @Override
    protected String getCommand() {
        return "jps | awk '{if ($2 == \"NCDriver\" || $2 == \"CCDriver\") print $1;}' | xargs -n 1 kill -9";
    }

}
