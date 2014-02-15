package edu.uci.ics.asterix.experiment.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class SocketTweetGeneratorDriver {
    public static void main(String[] args) throws Exception {
        SocketTweetGeneratorConfig clientConfig = new SocketTweetGeneratorConfig();
        CmdLineParser clp = new CmdLineParser(clientConfig);
        try {
            clp.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            clp.printUsage(System.err);
            System.exit(1);
        }

        if ((clientConfig.getDataInterval() == -1 && clientConfig.getDuration() == -1)
                || (clientConfig.getDataInterval() > 0 && clientConfig.getDuration() > 0)) {
            System.err.println("Must use exactly one of -d or -di");
            clp.printUsage(System.err);
            System.exit(1);
        }

        SocketTweetGenerator client = new SocketTweetGenerator(clientConfig);
        client.start();
    }
}
