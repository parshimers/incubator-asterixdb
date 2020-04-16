package org.apache.asterix.external.library;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibrary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JavaLibrary implements ILibrary<ClassLoader> {

    private final ExternalLibraryClassLoader cl;

    private static final Logger LOGGER = LogManager.getLogger();

    public JavaLibrary(String libraryPath) throws AsterixException, MalformedURLException {

        File installDir = new File(libraryPath);

        // get a reference to the specific library dir
        File libDir = installDir;

        FilenameFilter jarFileFilter = (dir, name) -> name.endsWith(".jar");

        // Get the jar file <Allow only a single jar file>
        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new AsterixException("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length <= 0) {
            throw new AsterixException("Incorrect library structure: could not find library jar");
        }

        File libJar = new File(libDir, jarsInLibDir[0]);
        // get the jar dependencies
        File libDependencyDir = new File(libDir.getAbsolutePath() + File.separator + "lib");
        int numDependencies = 1;
        String[] libraryDependencies = null;
        if (libDependencyDir.exists()) {
            libraryDependencies = libDependencyDir.list(jarFileFilter);
            numDependencies += libraryDependencies.length;
        }

        ClassLoader parentClassLoader = JavaLibrary.class.getClassLoader();
        URL[] urls = new URL[numDependencies];
        int count = 0;
        // get url of library
        urls[count++] = libJar.toURI().toURL();

        // get urls for dependencies
        if (libraryDependencies != null && libraryDependencies.length > 0) {
            for (String dependency : libraryDependencies) {
                File file = new File(libDependencyDir + File.separator + dependency);
                urls[count++] = file.toURI().toURL();
            }
        }

        // create and return the class loader
        this.cl = new ExternalLibraryClassLoader(urls, parentClassLoader);

    }

    public ClassLoader get() {
        return cl;
    }

    public void close() {
        try {
            if (cl != null) {
                cl.close();
            }
        } catch (IOException e) {
            LOGGER.error("Couldn't close classloader", e);
        }
    }

}
