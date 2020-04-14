package org.apache.asterix.common.library;

import org.apache.asterix.common.metadata.DataverseName;

import java.net.URL;

public class PythonLibraryManager {

    URL[] getLibraryUrls(DataverseName dataverseName, String libraryName);

    void setLibraryPath(DataverseName dataverseName, String libraryName, URL[] urls);
}
