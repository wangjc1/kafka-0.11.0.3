package org.apache.kafka.common.fileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertEquals;

/**
 * @author: wangjc
 * 2018/12/11
 * http://ifeve.com/file-channel/
 */
public class RandomAccessFileTest {
    String FILE_NAME = "./ra.txt";
    RandomAccessFile randomFile ;
    FileChannel channel;

    @Before
    public void before()  throws Exception{
        randomFile = new RandomAccessFile(FILE_NAME, "rw");
        /*
        打开FileChannel在使用FileChannel之前，必须先打开它。但是，我们无法直接打开一个FileChannel，
        需要通过使用一个InputStream、OutputStream或RandomAccessFile来获取一个FileChannel实例。
        下面是通过RandomAccessFile打开
        */
        channel = randomFile.getChannel();
    }

    @Test
    public void testReadAndWrite() throws Exception{
        ByteBuffer buf = ByteBuffer.allocate(48);
        int bCount = channel.read(buf);

        String newData = "New String to write to file..." + System.currentTimeMillis();
        buf.clear();
        buf.put(newData.getBytes());
        buf.flip();

        //将数据写入到通道中，这时可能还没写文件
        while(buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    @Test
    public void testPosition() throws Exception{
        ByteBuffer buf = ByteBuffer.allocate(48);
        String newData = "New String to write to file..." + System.currentTimeMillis();
        buf.put(newData.getBytes());
        buf.flip();
        //将数据写入到通道中，这时可能还没写文件
        while(buf.hasRemaining()) {
            channel.write(buf);
        }

        ByteBuffer newBuf = ByteBuffer.allocate(newData.length());
        channel.position(0);//从开始读取
        channel.read(newBuf);
        assertEquals(newData,new String(newBuf.array()));
    }

    @Test
    public void testTruncate() throws Exception{
        ByteBuffer buf = ByteBuffer.allocate(48);
        String newData = "New String to write to file..." + System.currentTimeMillis();
        buf.put(newData.getBytes());
        buf.flip();
        //将数据写入到通道中，这时可能还没写文件
        while(buf.hasRemaining()) {
            channel.write(buf);
        }

        ByteBuffer newBuf = ByteBuffer.allocate(3);
        channel.truncate(3);//截取3个字符
        assertEquals(channel.position(),3);

        channel.position(0);
        channel.read(newBuf);
        assertEquals("New",new String(newBuf.array()));
    }

    @After
    public void after()  throws Exception{
        /*
        FileChannel.force()方法将通道里尚未写入磁盘的数据强制写到磁盘上。出于性能方面的考虑，
        操作系统会将数据缓存在内存中，所以无法保证写入到FileChannel里的数据一定会即时写到磁盘上。
        要保证这一点，需要调用force()方法。force()方法有一个boolean类型的参数，指明是否同时将
        文件元数据（权限信息等）写到磁盘上。
         */
        channel.force(true);
        channel.close();
        randomFile.close();
        new File(FILE_NAME).delete();
    }
}
