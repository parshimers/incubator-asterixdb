package org.apache.asterix.external.library;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.asterix.common.library.ILibrary;

public class PythonLibrary implements ILibrary<URL> {

    private final URL path;

    public PythonLibrary(String path) throws MalformedURLException {
        this.path = new File(path).toURL();
    }

    public void init() {

    }

    @Override
    public URL get() {
        return path;

    }

    public void close() {

    }

}
