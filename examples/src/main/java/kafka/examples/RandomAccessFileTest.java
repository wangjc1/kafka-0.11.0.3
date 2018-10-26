package kafka.examples;

import java.io.RandomAccessFile;

/**
 * @author: wangjc
 * 2018/10/25
 */
public class RandomAccessFileTest {

    public static void main(String[] args) throws Exception {
     /*   String file = "1.txt";
        RandomAccessFile randomFile = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomFile.getChannel();
        MappedByteBuffer mapBuf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 20);

        System.out.println(mapBuf.position());
        mapBuf.putLong(new Date().getTime());
        mapBuf.putInt(20);
        System.out.println(mapBuf.position());

        mapBuf.force();
        randomFile.close();*/

     String  file = "D:\\Work\\code\\scala-master\\kafka-0.11.0.3\\out\\server\\kafka-logs\\topic1-0\\00000000000000000024.index";
        RandomAccessFile rf =  new RandomAccessFile(file, "rw");
        rf.setLength(280);
        System.out.print(rf.read());


    }
}
