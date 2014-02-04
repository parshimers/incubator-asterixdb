package edu.uci.ics.asterix.experiment.client;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

public class SocketTweetGeneratorConfig {

    @Option(name = "-p", aliases = "--partition-range-start", usage = "Starting partition number for the set of data generators (default = 0)")
    private int partitionRangeStart = 0;

    public int getPartitionRangeStart() {
        return partitionRangeStart;
    }

    @Option(name = "-d", aliases = { "--duration" }, usage = "Duration in seconds to run data generation (default: 60)")
    private int duration = 60;

    public int getDuration() {
        return duration;
    }

    @Argument(required = true, usage = "A list of <ip>:<port> pairs (addresses) to send data to", metaVar = "addresses...", handler = AddressOptionHandler.class)
    private List<Pair<String, Integer>> addresses;

    public List<Pair<String, Integer>> getAddresses() {
        return addresses;
    }

    public static class AddressOptionHandler extends OptionHandler<Pair<String, Integer>> {

        public AddressOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Pair<String, Integer>> setter) {
            super(parser, option, setter);
        }

        @Override
        public int parseArguments(Parameters params) throws CmdLineException {
            int counter = 0;
            while (true) {
                String param;
                try {
                    param = params.getParameter(counter);
                } catch (CmdLineException ex) {
                    break;
                }

                String[] hostPort = param.split(":");
                if (hostPort.length != 2) {
                    throw new CmdLineException("Invalid address: " + param + ". Expected <host>:<port>");
                }
                Integer port = null;
                try {
                    port = Integer.parseInt(hostPort[1]);
                } catch (NumberFormatException e) {
                    throw new CmdLineException("Invalid port " + hostPort[1] + " for address " + param + ".");
                }
                setter.addValue(Pair.of(hostPort[0], port));
                counter++;
            }
            return counter;
        }

        @Override
        public String getDefaultMetaVariable() {
            return "addresses";
        }

    }
}
