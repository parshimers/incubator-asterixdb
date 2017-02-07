package org.apache.hyracks.storage.am.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class IOManagerPathTest {
    @Test
    public void test() throws HyracksDataException {
        IODeviceHandle shorter = new IODeviceHandle(new File("/tmp/1"), "storage");
        IODeviceHandle longer = new IODeviceHandle(new File("/tmp/11"), "storage");
        IOManager ioManager = new IOManager(Arrays.asList(new IODeviceHandle[] { shorter, longer }));
        FileReference f = ioManager.resolveAbsolutePath("/tmp/11/storage/Foo_idx_foo/my_btree");
        Assert.assertEquals("/tmp/11/storage/Foo_idx_foo/my_btree", f.getAbsolutePath());
    }
}
