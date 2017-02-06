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
 */
public class TailSourceNG extends AbstractSource implements Configurable, EventDrivenSource {
    public static final Logger logger = LoggerFactory.getLogger(TailSourceNG.class);
    private TailSource tail;
    private String monitorFile;
    private String fileEncode;
    private int batchSize;
    private boolean startFromEnd;

    private SourceCounter counter;

    private String delimRegex;
    private String delimMode;

    @Override
    public void configure(Context context) {
        this.monitorFile = context.getString("monitorFile");
        this.fileEncode = context.getString("fileEncode", "UTF-8");
        this.batchSize = context.getInteger("batchSize", 100);
        this.startFromEnd = context.getBoolean("startFromEnd", true);
        this.delimRegex = context.getString("delimRegex", null);
        this.delimMode = context.getString("delimMode", null);

        Preconditions.checkNotNull(monitorFile == null, "Monitoring file is null!!!");

        if (counter == null) {
            counter = new SourceCounter(getName());
        }

    }


    @Override
    public void start() {
        logger.info("Starting {}...", this);
        File f = new File(monitorFile);
        // 100 ms between checks
        this.tail = new TailSource(100);
        // Add a new file to the multi tail.
        Cursor c;
        if (delimRegex == null) {
            if (startFromEnd) {
                // init cursor positions on first dir check when startFromEnd is set
                // to true
                c = new Cursor(this, counter, f, f.length(), f.length(), f
                        .lastModified(), fileEncode, batchSize);
            } else {
                c = new Cursor(this, counter, f, fileEncode, batchSize);
            }
        } else {
            if (startFromEnd) {
                // init cursor positions on first dir check when startFromEnd is set
                // to true
                c = new CustomDelimCursor(this, counter, f, fileEncode, batchSize, f.length(), f.length(), f.lastModified(), delimRegex, delimMode);
            } else {
                c = new CustomDelimCursor(this, counter, f, fileEncode, batchSize, delimRegex, delimMode);
            }
        }

        try {
            c.initCursorPos();
        } catch (InterruptedException e) {
            logger.error("Initializing of custom delimiter cursor failed", e);
            c.close();
            return;
        }

        tail.addCursor(c);
        tail.open();
        super.start();
        counter.start();
        logger.info("TailDir source started");
    }


    @Override
    public void stop() {
        tail.close();
        counter.stop();
        super.stop();
        logger.info("TailDir source stopped");
    }

}
