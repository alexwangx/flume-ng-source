package com.source.tailDir;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MonitorDirTest {
    private static final Logger logger = LoggerFactory.getLogger(MonitorDirTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        MonitorDirTest test = new MonitorDirTest();
        test.testFileMonitor("/Users/alex/Downloads/testDir/test");
        Thread.sleep(1000000);
    }

    public void testFileMonitor(String dir) {
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
        } catch (FileSystemException e) {
            return;
        }
        DefaultFileMonitor fileMonitor = new DefaultFileMonitor(new MonitorRunnableTest());
        fileMonitor.setRecursive(false);

        FileObject fileObject = null;

        try {
            fileObject = fsManager.resolveFile(dir);
        } catch (FileSystemException e) {
        }

        fileMonitor.addFile(fileObject);
        fileMonitor.start();
    }

    class MonitorRunnableTest implements FileListener {

        @Override
        public void fileCreated(FileChangeEvent event) throws Exception {
            logger.debug("in file create , file name is :{}", event.getFile());
        }

        @Override
        public void fileDeleted(FileChangeEvent event) throws Exception {
            logger.debug("in file delete , file name is :{}", event.getFile());
        }

        @Override
        public void fileChanged(FileChangeEvent event) throws Exception {
            logger.debug("in file changed , file name is :{}", event.getFile());
        }

    }
}