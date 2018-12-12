package org.apache.kafka.common.fileChannel;

import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author: wangjc
 * 2018/12/11
 */
public class MappedByteBufferTest{

    @Test
    public void testReadAndWrite() throws Exception{
        System.out.println(new File(".").getAbsolutePath());
        String srcFile = "./clients.iml";
        String destFile = "./clients.txt";
        RandomAccessFile rafi = new RandomAccessFile(srcFile, "r");
        RandomAccessFile rafo = new RandomAccessFile(destFile, "rw");
        FileChannel fci = rafi.getChannel();
        FileChannel fco = rafo.getChannel();
        long size = fci.size();
        byte b;
        long start = System.currentTimeMillis();
        MappedByteBuffer mbbi = fci.map(FileChannel.MapMode.READ_ONLY, 0, size);
        System.out.println("output: " + (double) (System.currentTimeMillis() - start) / 1000 + "s");

        MappedByteBuffer mbbo = fco.map(FileChannel.MapMode.READ_WRITE, 0, size);
        start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            b = mbbi.get(i);
            mbbo.put(i, b);
        }

        fci.close();
        fco.close();
        rafi.close();
        rafo.close();
        System.out.println("input: " + (double) (System.currentTimeMillis() - start) / 1000 + "s");
    }

}
