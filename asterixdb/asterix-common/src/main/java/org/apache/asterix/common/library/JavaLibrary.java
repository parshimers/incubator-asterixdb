package org.apache.asterix.common.library;

import org.apache.asterix.common.metadata.DataverseName;

public class JavaLibrary {

    /**
     * Finds a class loader for a given pair of dataverse name and library name.
     *
     * @param dataverseName
     * @param libraryName
     * @return the library class loader associated with the dataverse and library.
     */
    ClassLoader getLibraryClassLoader(DataverseName dataverseName, String libraryName);
}
