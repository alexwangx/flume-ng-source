package com.source.tailDir;

import com.source.tailDir.dirwatchdog.DirChangeHandler;
import com.source.tailDir.dirwatchdog.DirWatcher;
import com.source.tailDir.dirwatchdog.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DirWatcherTest {
    private static final Logger LOG = LoggerFactory.getLogger(DirWatcherTest.class);

    private DirWatcher watcher;

    public static void main(String[] args) {
        DirWatcherTest dwt = new DirWatcherTest();
        try {
            dwt.setUp();
            dwt.tearDown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setUp() throws InterruptedException {

        watcher = createWatcher(new File("/Users/alex/Downloads/testDir/test"), "group-[^\\\\.]*", 0);
        watcher.start();
        Thread.sleep(1000000);
    }

    public void tearDown() throws InterruptedException {
        watcher.stop();
    }

    public DirWatcher createWatcher(File dir, final String regex,
                                    final int recurseDepth) {
        DirWatcher watcher = new DirWatcher(dir, new RegexFileFilter(regex), 250);
        watcher.addHandler(new DirChangeHandler() {

            @Override
            public void fileCreated(File f) {
                System.out.println("handling created of file " + f);

            }

            @Override
            public void fileDeleted(File f) {
                System.out.println("handling deletion of file " + f);

            }

        });
        return watcher;
    }
}
