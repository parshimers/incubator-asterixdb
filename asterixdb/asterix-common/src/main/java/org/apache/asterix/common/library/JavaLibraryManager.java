package org.apache.asterix.common.library;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.net.URLClassLoader;
import java.util.List;

public class JavaLibraryManager {


    /**
     * Registers the library class loader with the external library manager.
     * <code>dataverseName</code> and <code>libraryName</code> uniquely identifies a class loader.
     *
     * @param dataverseName
     * @param libraryName
     * @param classLoader
     */
    void registerLibraryClassLoader(DataverseName dataverseName, String libraryName, URLClassLoader classLoader)
            throws HyracksDataException;

    /**
     * @return all registered libraries.
     */
    List<Pair<DataverseName, String>> getAllLibraries();

    /**
     * De-registers a library class loader.
     *
     * @param dataverseName
     * @param libraryName
     */
    void deregisterLibraryClassLoader(DataverseName dataverseName, String libraryName);

    /**
     * Finds a class loader for a given pair of dataverse name and library name.
     *
     * @param dataverseName
     * @param libraryName
     * @return the library class loader associated with the dataverse and library.
     */
    ClassLoader getLibraryClassLoader(DataverseName dataverseName, String libraryName);
}
