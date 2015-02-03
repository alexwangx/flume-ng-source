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

/**
 * tail a file on unix platform, replacing tail -F command
 *
 */
public class TailSourceNG extends AbstractSource implements Configurable, EventDrivenSource {
    public static final Logger logger = LoggerFactory.getLogger(TailSourceNG.class);
    private TailSource tail;
    private String monitorFile;
    private String fileEncode;
    private int batchSize;
    private boolean startFromEnd;

    public SourceCounter sourceCounter;


    @Override
    public void configure(Context context) {

        this.monitorFile = context.getString("monitorFile");
        this.fileEncode = context.getString("fileEncode", "UTF-8");
        this.batchSize = context.getInteger("batchSize", 100);
        this.startFromEnd = context.getBoolean("startFromEnd", true);
        Preconditions.checkNotNull(monitorFile == null, "Monitoring file is null!!!");

        logger.debug("in configure , monitorFile value is :{}", monitorFile);
        logger.debug("in configure , fileEncode value is :{}", fileEncode);
        logger.debug("in configure , batchSize value is :{}", batchSize);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

    }


    @Override
    public void start() {
        File f = new File(monitorFile);
        // 100 ms between checks
        this.tail = new TailSource(100);
        // Add a new file to the multi tail.
        Cursor c;
        if (startFromEnd) {
            // init cursor positions on first dir check when startFromEnd is set
            // to true
            c = new Cursor(this, sourceCounter, f, f.length(), f.length(), f
                    .lastModified(), fileEncode, batchSize);
            try {
                c.initCursorPos();
            } catch (InterruptedException e) {
                logger.error("Initializing of cursor failed", e);
                c.close();
                return;
            }
        } else {
            c = new Cursor(this, sourceCounter, f, fileEncode, batchSize);
        }
        tail.addCursor(c);
        tail.open();
        super.start();
        sourceCounter.start();
        logger.info("TailDir source started");
    }


    @Override
    public void stop() {
        tail.close();
        sourceCounter.stop();
        super.stop();
        logger.info("TailDir source stopped");
    }

    @VisibleForTesting
    public SourceCounter getSourceCounter() {
        return sourceCounter;
    }
}
