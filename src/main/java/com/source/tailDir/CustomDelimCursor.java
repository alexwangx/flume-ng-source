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

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomDelimCursor extends Cursor {
    public static final Logger logger = LoggerFactory.getLogger(CustomDelimCursor.class);

    final DelimMode delimMode;
    final Pattern pat;
    byte[] prefix = null; // only for include delimiter in next mode

    /**
     * Are deliminter sequences part of the previous, completed excluded or part
     * of the next event record.
     */
    enum DelimMode {
        INCLUDE_PREV, EXCLUDE, INCLUDE_NEXT
    }

    CustomDelimCursor(AbstractSource source, SourceCounter sourceCounter, File f, String charEncode, int batchSize, long lastReadOffset, long lastFileLen, long lastMod, String regex, String delimModeStr) {
        super(source, sourceCounter, f, lastReadOffset, lastFileLen, lastMod, charEncode, batchSize);
        this.pat = Pattern.compile(regex);
        this.delimMode = extractDelim(delimModeStr);
    }

    CustomDelimCursor(AbstractSource source, SourceCounter sourceCounter, File f, String charEncode, int batchSize, String regex, String delimModeStr) {
        super(source, sourceCounter, f, charEncode, batchSize);
        this.pat = Pattern.compile(regex);
        this.delimMode = extractDelim(delimModeStr);
    }


    private DelimMode extractDelim(String delimModeStr) {
        DelimMode delimMode = DelimMode.EXCLUDE; // default to exclude mode
        if (delimModeStr != null) {
            if ("exclude".equals(delimModeStr)) {
                delimMode = DelimMode.EXCLUDE;
            } else if ("prev".equals(delimModeStr)) {
                delimMode = DelimMode.INCLUDE_PREV;
            } else if ("next".equals(delimModeStr)) {
                delimMode = DelimMode.INCLUDE_NEXT;
            }
        }
        return delimMode;
    }


    /**
     * This is a cheat class to present a CharSequence interface backed by a
     * byte[]/ByteBuffer. This allows us to use regexes against the byte[]. Note,
     * we are puposely avoiding character encoding here.
     */
    public static class ByteBufferAsCharSequence implements CharSequence {
        ByteBuffer buf;

        ByteBufferAsCharSequence(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public char charAt(int arg0) {
            return (char) buf.get(arg0);
        }

        @Override
        public int length() {
            return buf.remaining();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            if (start < 0 || end < 0 || end > length() || start > end) {
                throw new IndexOutOfBoundsException("Index out of bounds start="
                        + start + " end=" + end);
            }
            byte[] bs = new byte[end - start];
            for (int i = start, j = 0; i < end; i++, j++) {
                bs[j] = buf.get(i);
            }

            return new ByteBufferAsCharSequence(ByteBuffer.wrap(bs));
        }
    }

    /**
     * Note, these version is less efficient than then default '\n' searching
     * version because it calls the compact call more often and also relies on
     * regexes instead of a simple char matches.
     */
    boolean extractLines(ByteBuffer buf) throws IOException, InterruptedException {
        boolean madeProgress = false;
        int start = buf.position();
        byte[] nextPrefix = null;
        buf.mark();
        final List<Event> eventList = new ArrayList<Event>();
        while (buf.hasRemaining()) {
            CharSequence cs = new ByteBufferAsCharSequence(buf);
            Matcher m = pat.matcher(cs);

            if (!m.find()) {
                // if not found, eat it all and go into drop mode.
                buf.reset();
                buf.compact(); // shift leftovers to front.
                return madeProgress;
            }

            int beg = m.start();
            int end = m.end();

            byte[] body = null;
            switch (delimMode) {
                case INCLUDE_PREV:
                    body = new byte[end];
                    buf.reset();
                    buf.get(body);
                    buf.mark();
                    buf.compact();
                    buf.flip();
                    buf.mark();
                    break;
                case EXCLUDE:
                    body = new byte[beg];
                    buf.reset();
                    buf.get(body);
                    buf.get(new byte[end - beg]); // toss bytes
                    buf.mark();
                    buf.compact();
                    buf.flip();
                    buf.mark();
                    break;
                case INCLUDE_NEXT:
                    // special case
                    body = new byte[beg];
                    buf.get(body);
                    nextPrefix = new byte[end - beg]; // keep prefix
                    buf.get(nextPrefix);
                    buf.mark();
                    buf.compact();
                    buf.flip();
                    buf.mark();
                    break;
            }

            if (prefix != null) {
                byte[] body2 = new byte[prefix.length + body.length];
                int i = 0;
                for (; i < prefix.length; i++) {
                    body2[i] = prefix[i];
                }
                for (int j = 0; j < body.length; j++, i++) {
                    body2[i] = body[j];
                }
                body = body2;
            }
            prefix = nextPrefix;
            nextPrefix = null;

            logger.debug("line info >>>>>>>>>" + new String(body, charEncode));
            synchronized (eventList) {
                sourceCounter.incrementEventReceivedCount();
                eventList.add(EventBuilder.withBody(new String(body, charEncode).getBytes()));
                if (eventList.size() >= batchSize || timeout()) {
                    flushEventBatch(eventList);
                }
            }

            madeProgress = true;

        }

        synchronized (eventList) {
            if (!eventList.isEmpty()) {
                flushEventBatch(eventList);
            }
        }

        // rewind for any left overs
        buf.reset();
        buf.compact(); // shift leftovers to front.
        return madeProgress;
    }

    /**
     * Flush any buffering the cursor has done. If the buffer does not end with
     * the proper delimiter, the remainder will get turned into a new event.
     * <p/>
     * This assumes that any remainders in buf are a single event -- this
     * corresponds to the last lines of files that do not end with the proper
     * delimiter.
     * <p/>
     * Also, prefix needs to be attached.
     */
    void flush() throws InterruptedException {
        if (raf != null) {
            try {
                raf.close(); // release handles
            } catch (IOException e) {
                logger.error("problem closing file " + e.getMessage(), e);
            }
        }

        buf.flip(); // buffer consume mode
        int remaining = buf.remaining();
        if (remaining > 0) {
            byte[] body = new byte[remaining];
            buf.get(body, 0, remaining); // read all data

            // include prefix if present.
            if (prefix != null) {
                byte[] body2 = new byte[prefix.length + body.length];
                int i = 0;
                for (; i < prefix.length; i++) {
                    body2[i] = prefix[i];
                }
                for (int j = 0; j < body.length; j++, i++) {
                    body2[i] = body[j];
                }
                body = body2;
            }

            try {
                logger.debug("flush method line info >>>>> " + new String(body, charEncode));
                sourceCounter.incrementAppendBatchReceivedCount();
                Event event = EventBuilder.withBody(new String(body, charEncode).getBytes());
                source.getChannelProcessor().processEvent(event);
                sourceCounter.incrementAppendBatchAcceptedCount();
            } catch (UnsupportedEncodingException e) {
                logger.error("UnsupportedEncodingException! " + e.getMessage(), e);
            }
        }
        in = null;
        buf.clear();
    }


}
