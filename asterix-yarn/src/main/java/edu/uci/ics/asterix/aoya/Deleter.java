package edu.uci.ics.asterix.aoya;

import java.io.File;

public class Deleter {
    public static void main(String[] args) {
        for (int i = 1; i < args.length; i++) {
            File f = new File(args[i]);
            f.delete();
        }
    }
}
