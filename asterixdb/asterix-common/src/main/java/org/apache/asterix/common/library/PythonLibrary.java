package org.apache.asterix.common.library;

import org.apache.asterix.common.metadata.DataverseName;

import java.net.URL;
import java.util.List;

public class PythonLibrary implements ILibrary<List<URL>>{

    List<URL> paths;


   public List<URL> get(){
       return paths;

   }

   public void set(List<URL> urls){
       this.paths = urls;
    }

}
