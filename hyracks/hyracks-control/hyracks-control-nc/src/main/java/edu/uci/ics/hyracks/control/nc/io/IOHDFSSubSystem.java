package edu.uci.ics.hyracks.control.nc.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import edu.uci.ics.hyracks.api.io.FileReference;

public class IOHDFSSubSystem implements IIOSubSystem {
    private static URI uri = null;
    static {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.namenode.replication.considerLoad","false");
        conf.addResource(new Path("config/core-site.xml"));
        conf.addResource(new Path("config/hdfs-site.xml"));
        conf.addResource(new Path("config/mapred-site.xml"));
        try {
            uri = new URI("hdfs://127.0.1.1:9000");
            fs = FileSystem.get(uri, conf);
        } catch (IOException | URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private static FileSystem fs;

    @Override
    public boolean exists(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.exists(new Path(uri.toString() + fileRef.getPath()));
    }

    @Override
    public boolean mkdirs(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.mkdirs(new Path(uri.toString() + fileRef.getPath()));
    }

    @Override
    public boolean delete(FileReference fileRef, boolean recursive) throws IllegalArgumentException, IOException {
        return fs.delete(new Path(uri.toString() + fileRef.getPath()), recursive);
    }

    @Override
    public boolean deleteOnExit(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.deleteOnExit(new Path(uri.toString() + fileRef.getPath()));
    }

    @Override
    public boolean isDirectory(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.isDirectory(new Path(uri.toString()+fileRef.getPath()));
    }

    @Override
    public String[] listFiles(FileReference fileRef, FilenameFilter filter) throws FileNotFoundException, IllegalArgumentException, IOException {
        ArrayList<String> files = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(uri.toString() + fileRef.getPath()), false);
        while(it.hasNext()) {
            LocatedFileStatus fileStatus = it.next();
            if(filter.accept(new File(Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath().getParent()).toString()), fileStatus.getPath().getName())) files.add(fileStatus.getPath().getName());
        }
        String tmp[] = new String[files.size()];
        tmp = files.toArray(tmp);
        return tmp;
    }

}
