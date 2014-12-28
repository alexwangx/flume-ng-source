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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This "tail"s a filename. Like a unix tail utility, it will wait for more
 * information to come to the file and periodically dump data as it is written.
 * It assumes that each line is a separate event.
 * <p/>
 * This is for legacy log files where the file system is the only mechanism
 * flume has to get events. It assumes that there is one entry per line (per
 * \n). If a file currently does not end with \n, it will remain buffered
 * waiting for more data until either a different file with the same name has
 * appeared, or the tail source is closed.
 * <p/>
 * It also has logic to deal with file rotations -- if a file is renamed and
 * then a new file is created, it will shift over to the new file. The current
 * file is read until the file pointer reaches the end of the file. It will wait
 * there until periodic checks notice that the file has become longer. If the
 * file "shrinks" we assume that the file has been replaced with a new log file.
 * <p/>
 * TODO (jon) This is not perfect.
 * <p/>
 * This reads bytes and does not assume any particular character encoding other
 * than that entry are separated by new lines ('\n').
 * <p/>
 * There is a possibility for inconsistent conditions when logs are rotated.
 * <p/>
 * 1) If rotation periods are faster than periodic checks, a file may be missed.
 * (this mimics gnu-tail semantics here)
 * <p/>
 * 2) Truncations of files will reset the file pointer. This is because the Java
 * file api does not a mechanism to get the inode of a particular file, so there
 * is no way to differentiate between a new file or a truncated file!
 * <p/>
 * 3) If a file is being read, is moved, and replaced with another file of
 * exactly the same size in a particular window and the last mod time of the two
 * are identical (this is often at the second granularity in FS's), the data in
 * the new file may be lost. If the original file has been completely read and
 * then replaced with a file of the same length this problem will not occur.
 * (See TestTailSource.readRotatePrexistingFailure vs
 * TestTailSource.readRotatePrexistingSameSizeWithNewModetime)
 * <p/>
 * Ideally this would use the inode number of file handle number but didn't find
 * java api to get these, or Java 7's WatchService file watcher API.
 */
public class TailSource {
    private static final Logger LOG = LoggerFactory.getLogger(TailSource.class);

    private static int thdCount = 0;
    private volatile boolean done = false;

    private final long sleepTime; // millis
    final List<Cursor> cursors = new ArrayList<Cursor>();
    private final List<Cursor> newCursors = new ArrayList<Cursor>();
    private final List<Cursor> rmCursors = new ArrayList<Cursor>();

    private TailThread thd = null;

    /**
     * This creates an empty tail source. It expects something else to add cursors
     * to it
     */
    public TailSource(long waitTime) {
        this.sleepTime = waitTime;
    }

    /**
     * This is the main driver thread that runs through the file cursor list
     * checking for updates and sleeping if there are none.
     */
    class TailThread extends Thread {

        TailThread() {
            super("TailThread-" + thdCount++);
        }

        @Override
        public void run() {
            try {
                // initialize based on initial settings.
                for (Cursor c : cursors) {
                    c.initCursorPos();
                }

                while (!done) {
                    synchronized (newCursors) {
                        cursors.addAll(newCursors);
                        newCursors.clear();
                    }

                    synchronized (rmCursors) {
                        cursors.removeAll(rmCursors);
                        for (Cursor c : rmCursors) {
                            c.flush();
                        }
                        rmCursors.clear();
                    }

                    boolean madeProgress = false;
                    for (Cursor c : cursors) {
                        LOG.debug("Progress loop: " + c.file);
                        if (c.tailBody()) {
                            madeProgress = true;
                        }
                    }

                    if (!madeProgress) {
                        Thread.sleep(sleepTime);
                    }
                }
                LOG.debug("Tail got done flag");
            } catch (InterruptedException e) {
                LOG.error("Tail thread nterrupted: " + e.getMessage(), e);
            } finally {
                LOG.info("TailThread has exited");
            }

        }
    }

    /**
     * Add another file Cursor to tail concurrently.
     */
    public synchronized void addCursor(Cursor cursor) {
        Preconditions.checkArgument(cursor != null);

        if (thd == null) {
            cursors.add(cursor);
            LOG.debug("Unstarted Tail has added cursor: " + cursor.file.getName());

        } else {
            synchronized (newCursors) {
                newCursors.add(cursor);
            }
            LOG.debug("Tail added new cursor to new cursor list: "
                    + cursor.file.getName());
        }

    }

    /**
     * Remove an existing cursor to tail.
     */
    synchronized public void removeCursor(Cursor cursor) {
        Preconditions.checkArgument(cursor != null);
        if (thd == null) {
            cursors.remove(cursor);
        } else {

            synchronized (rmCursors) {
                rmCursors.add(cursor);
            }
        }

    }

    public void close() {
        synchronized (this) {
            done = true;
            if (thd == null) {
                LOG.warn("TailSource double closed");
                return;
            }
            try {
                while (thd.isAlive()) {
                    thd.join(100L);
                    thd.interrupt();
                }
            } catch (InterruptedException e) {
                LOG.error("Tail source Interrupted Exception: " + e.getMessage(), e);
            }
            thd = null;
        }
    }

    synchronized public void open() {
        if (thd != null) {
            throw new IllegalStateException("Attempted to open tail source twice!");
        }
        thd = new TailThread();
        thd.start();
    }


}
