package com.source.tailDir;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TailDirSourceNGTest {
    private TailDirSourceNG source;
    private Context context;
    static MemoryChannel channel;
    private File tmpDir;

    @Before
    public void setUp() throws InterruptedException {
        tmpDir = Files.createTempDir();
        source = new TailDirSourceNG();
        channel = new MemoryChannel();
        context = new Context();

        context.put("monitorPath", tmpDir.getAbsolutePath());
        context.put("fileEncode", "GBK");
        context.put("batchSize", "5");
//        context.put("fileregex", "group-[^\\\\.]*");
        context.put("fileregex", ".*");
        context.put("startFromEnd", "false");

        Configurables.configure(channel, context);

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);
        source.setChannelProcessor(new ChannelProcessor(rcs));

    }

    @After
    public void tearDown() throws InterruptedException {
        source.stop();
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();
    }

    @Test
    public void testProcess() throws InterruptedException, LifecycleException,
            EventDeliveryException, IOException {
        File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

        Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                        "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);
        Configurables.configure(source, context);
        source.start();

//          while (source.getSourceCounter().getEventAcceptedCount() < 8) {
//              Thread.sleep(10);
//          }
        Transaction txn = channel.getTransaction();
        txn.begin();
        Event e = channel.take();
        Assert.assertNotNull("Event must not be null", e);
        Assert.assertArrayEquals("file1line1".getBytes(), e.getBody());
        txn.commit();
        txn.close();
    }
}
