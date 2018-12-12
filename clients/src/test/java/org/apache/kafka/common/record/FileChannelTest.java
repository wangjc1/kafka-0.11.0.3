package org.apache.kafka.common.record;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.kafka.test.TestUtils.tempFile;

/**
 * @author: wangjc
 * 2018/11/15
 */
public class FileChannelTest{
    private FileChannel channel;

    @Before
    public void before() throws IOException {
        RandomAccessFile in = new RandomAccessFile(tempFile(),"rw");
        channel = in.getChannel();
    }

    @Test
    public void writeTest() throws Exception {
        ByteBuffer buff = ByteBuffer.allocate(4096);
        buff.put("hello".getBytes());
        buff.flip();
        channel.write(buff);

        Thread.sleep(10000);
    }

}
