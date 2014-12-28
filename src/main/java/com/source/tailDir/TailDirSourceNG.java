/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.source.tailDir;

import com.source.tailDir.dirwatchdog.DirChangeHandler;
import com.source.tailDir.dirwatchdog.DirWatcher;
import com.source.tailDir.dirwatchdog.RegexFileFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * This source tails all the file in a directory that match a specified regular
 * expression.
 */
public class TailDirSourceNG extends AbstractSource implements Configurable, EventDrivenSource {
    public static final Logger logger = LoggerFactory.getLogger(TailDirSourceNG.class);
    private TailSource tail;
    private DirWatcher watcher;
    private volatile boolean dirChecked = false;

    private String monitorDirPath;
    private String fileEncode;
    private String fileRegex;
    private int batchSize;
    private boolean startFromEnd;

    public SourceCounter sourceCounter;


    @Override
    public void configure(Context context) {

        this.monitorDirPath = context.getString("monitorPath");
        this.fileEncode = context.getString("fileEncode", "UTF-8");
        this.fileRegex = context.getString("fileRegex", ".*");
        this.batchSize = context.getInteger("batchSize", 20);
        this.startFromEnd = context.getBoolean("startFromEnd", true);
        Preconditions.checkNotNull(monitorDirPath == null, "Monitoring directory path is null!!!");

        logger.debug("in configure , monitorDirPath value is :{}", monitorDirPath);
        logger.debug("in configure , fileEncode value is :{}", fileEncode);
        logger.debug("in configure , fileRegex value is :{}", fileRegex);
        logger.debug("in configure , batchSize value is :{}", batchSize);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

    }


    @Override
    public void start() {
        Preconditions.checkState(watcher == null,
                "Attempting to open an already open TailDirSource (" + monitorDirPath + ", \""
                        + fileRegex + "\")");
        // 100 ms between checks
        this.tail = new TailSource(100);
        watcher = createWatcher(new File(monitorDirPath), fileRegex, this, sourceCounter);
        dirChecked = true;
        watcher.start();
        tail.open();
        super.start();
        sourceCounter.start();
        logger.info("TailDir source started");
    }


    @Override
    public void stop() {
        tail.close();
        this.watcher.stop();
        this.watcher = null;
        sourceCounter.stop();
        super.stop();
        logger.info("TailDir source stopped");
    }

    private DirWatcher createWatcher(File dir, final String regex, final TailDirSourceNG source, final SourceCounter sourceCounter) {
        // 250 ms between checks
        DirWatcher watcher = new DirWatcher(dir, new RegexFileFilter(regex), 250);
        watcher.addHandler(new DirChangeHandler() {
            Map<String, Cursor> curmap = new HashMap<String, Cursor>();

            @Override
            public void fileCreated(File f) {
                if (f.isDirectory()) {
                    return;
                }

                // Add a new file to the multi tail.
                logger.info("added file " + f);
                Cursor c;
                if (startFromEnd && !dirChecked) {
                    // init cursor positions on first dir check when startFromEnd is set
                    // to true
                    c = new Cursor(source, sourceCounter, f, f.length(), f.length(), f
                            .lastModified(), fileEncode, batchSize);
                    try {
                        c.initCursorPos();
                    } catch (InterruptedException e) {
                        logger.error("Initializing of cursor failed", e);
                        c.close();
                        return;
                    }
                } else {
                    c = new Cursor(source, sourceCounter, f, fileEncode, batchSize);
                }
                curmap.put(f.getPath(), c);
                tail.addCursor(c);
            }

            @Override
            public void fileDeleted(File f) {
                logger.debug("handling deletion of file " + f);
                String fileName = f.getPath();
                Cursor c = curmap.remove(fileName);
                // this check may seem unneeded but there are cases which it handles,
                // e.g. if unwatched subdirectory was removed c is null.
                if (c != null) {
                    logger.info("removed file " + f);
                    tail.removeCursor(c);
                }
            }

        });

        // Separate check is needed to init cursor positions
        // (to the end of the files in dir)
        if (startFromEnd) {
            watcher.check();
        }
        return watcher;
    }

    @VisibleForTesting
    public SourceCounter getSourceCounter() {
        return sourceCounter;
    }
}
