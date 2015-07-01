/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aoya;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class Deleter {
    private static final Log LOG = LogFactory.getLog(Deleter.class);

    public static void main(String[] args) throws IOException {

	LogManager.getRootLogger().setLevel(Level.DEBUG); 
	
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
