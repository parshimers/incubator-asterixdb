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
        SocketTweetGenerator client = new SocketTweetGenerator(clientConfig);
        client.start();
    }
}
