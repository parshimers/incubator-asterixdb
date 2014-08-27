package edu.uci.ics.asterix.aoya;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Deleter {
    private static final Log LOG = LogFactory.getLog(Deleter.class);

    public static void main(String[] args) throws IOException {
        LOG.info("Obliterator args: " + Arrays.toString(args));
        for (int i = 0; i < args.length; i++) {
            File f = new File(args[i]);
            if (f.exists()) {
                LOG.info("Deleting: " + f.getPath());
                FileUtils.deleteDirectory(f);
            } else {
                LOG.error("Could not find file to delete: " + f.getPath());
            }
        }
    }
}
