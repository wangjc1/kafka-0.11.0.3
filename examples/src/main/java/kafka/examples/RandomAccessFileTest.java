package kafka.examples;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

/**
 * @author: wangjc
 * 2018/10/25
 */
public class RandomAccessFileTest {

    public static void main(String[] args) throws Exception {
        String file = "d:\\1.txt";
        RandomAccessFile randomFile = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomFile.getChannel();
        MappedByteBuffer mapBuf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 20);

        System.out.println(mapBuf.position());
        mapBuf.putLong(new Date().getTime());
        mapBuf.putInt(20);
        System.out.println(mapBuf.position());
        mapBuf.force();
    }
}
