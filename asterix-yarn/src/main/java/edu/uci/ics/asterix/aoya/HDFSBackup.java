package edu.uci.ics.asterix.aoya;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class HDFSBackup {
    Configuration conf = new YarnConfiguration();
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    public static void main(String[] args) throws ParseException, IllegalArgumentException, IOException {
        HDFSBackup back = new HDFSBackup();
        Options opts = new Options();
        CommandLine cliParser = new GnuParser().parse(opts, args);
        @SuppressWarnings("unchecked")
        List<String> pairs = (List<String>) cliParser.getArgList();

        List<Path[]> sources = new ArrayList<Path[]>(10);
        for (String p : pairs) {
            String[] s = p.split(",");
            sources.add(new Path[] { new Path(s[0]), new Path(s[1]) });
        }

        try {
            back.performBackup(sources);
        } catch (IOException e) {
            back.LOG.fatal("Backup unsuccessful: " + e.getStackTrace().toString());
            System.exit(1);
        }
    }

    private void performBackup(List<Path[]> paths) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        for (Path[] p : paths) {
            fs.copyFromLocalFile(p[0], p[1]);
        }
    }
}
