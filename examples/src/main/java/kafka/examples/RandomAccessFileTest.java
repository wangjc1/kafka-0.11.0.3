package kafka.examples;

import java.io.RandomAccessFile;

/**
 * @author: wangjc
 * 2018/10/25
 */
public class RandomAccessFileTest {

    public static void main(String[] args) throws Exception {
        String file = "D:\\Work\\code\\scala-master\\kafka-0.11.0.3\\out\\server\\kafka-logs\\topic1-1";
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");

    }
}
