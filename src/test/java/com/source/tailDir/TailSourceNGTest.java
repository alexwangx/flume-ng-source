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

public class TailSourceNGTest {
    private TailSourceNG source;
    private Context context;
    static MemoryChannel channel;
    private File tmpDir;

    @Before
    public void setUp() throws InterruptedException {
        tmpDir = Files.createTempDir();
        source = new TailSourceNG();
        channel = new MemoryChannel();
        context = new Context();

        context.put("fileEncode", "GBK");
        context.put("batchSize", "8");
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
        context.put("monitorFile", f1.getAbsolutePath());

        Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                        "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);
        Configurables.configure(source, context);
        source.start();
        Transaction txn = channel.getTransaction();
        txn.begin();
//        for (int i = 0; i < 8; i++) {
//            System.out.println("================="+new String(channel.take().getBody()) + i+"");
//        }
        Event e = channel.take();
        Assert.assertNotNull("Event must not be null", e);
        Assert.assertArrayEquals("file1line1".getBytes(), e.getBody());
        txn.commit();
        txn.close();
    }

    @Test
    public void testCustomDelimProcess() throws InterruptedException, LifecycleException,
            EventDeliveryException, IOException {
        File f1 = new File(tmpDir.getAbsolutePath() + "/file2");
        context.put("monitorFile", f1.getAbsolutePath());

        context.put("delimRegex", "</Document>");
        context.put("delimMode", "prev");
//        context.put("delimRegex", "\\n+");
//        context.put("delimMode", "exclude");

//        Files.write("file1line1\n\n\n\n\nfile1line2\n\n\n\n\nfile1line3\n\n\n\n\n\nfile1line4\n\n\n\n\n\n" +
//                        "file1line5\n\n\n\n\n\nfile1line6\n\n\n\nfile1line7\nfile1line8\n",
//                f1, Charsets.UTF_8);

        for (int i = 0; i < 8; i++) {
            String str = "[2015-02-04 10:16:23.81]\n" +
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<Document xmlns=\"urn:cnaps:std:ibps:2010:tech:xsd:ccms.990.001.01\" xmlns:xsi=\" http://www.w3.org/2001/XMLSchema-instance\">\n" +
                    "    <ComuCnfm>\n" +
                    "        <OrigSndr>303100000006</OrigSndr>\n" +
                    "        <OrigSndDt>20150204</OrigSndDt>\n" +
                    "        <MsgTp>ibps.309.001.01</MsgTp>\n" +
                    "        <MsgId>00000000000000667781</MsgId>\n" +
                    "        <MsgRefId>00000000000000667781</MsgRefId>\n" +
                    "        <MsgProCd>0000" + i + "</MsgProCd>\n" +
                    "    </ComuCnfm>\n" +
                    "</Document>" +
                    "\n\n\n";
            Files.append(str, f1, Charsets.UTF_8);
        }

        Configurables.configure(source, context);
        source.start();

        Transaction txn = channel.getTransaction();
        txn.begin();
//        for (int i = 0; i < 8; i++) {
//            System.out.println("******************"+new String(channel.take().getBody()) + i+"");
//        }
        String str = "[2015-02-04 10:16:23.81]\n" +
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<Document xmlns=\"urn:cnaps:std:ibps:2010:tech:xsd:ccms.990.001.01\" xmlns:xsi=\" http://www.w3.org/2001/XMLSchema-instance\">\n" +
                "    <ComuCnfm>\n" +
                "        <OrigSndr>303100000006</OrigSndr>\n" +
                "        <OrigSndDt>20150204</OrigSndDt>\n" +
                "        <MsgTp>ibps.309.001.01</MsgTp>\n" +
                "        <MsgId>00000000000000667781</MsgId>\n" +
                "        <MsgRefId>00000000000000667781</MsgRefId>\n" +
                "        <MsgProCd>00000</MsgProCd>\n" +
                "    </ComuCnfm>\n" +
                "</Document>";
        Event e = channel.take();
        Assert.assertNotNull("Event must not be null", e);
        Assert.assertArrayEquals(str.getBytes(), e.getBody());
        txn.commit();
        txn.close();
    }
}
