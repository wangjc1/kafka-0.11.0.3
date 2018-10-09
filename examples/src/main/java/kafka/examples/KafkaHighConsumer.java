package kafka.examples;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Kafka High API
 * @author Wang
 */
@Deprecated //since 0.11.0.0. This class will be removed in a future release.
public class KafkaHighConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public KafkaHighConsumer(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //设计topic和stream的关系，即K为topic，V为stream的个数N
        topicCountMap.put(topic, new Integer(numThreads));
        topicCountMap.put("topic2", 2);
        topicCountMap.put("topic3",1);
        //获取numThreads个stream
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executor = Executors.newFixedThreadPool(numThreads);
        int threadNumber = 0;
        //开启N个消费组线程消费这N个stream
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerMsgTask(stream, threadNumber));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public static void main(String[] arg) {
        String[] args = {"localhost:2181", "topic-group", KafkaProperties.TOPIC, "4"};
        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);
        KafkaHighConsumer demo = new KafkaHighConsumer(zooKeeper, groupId, topic);
        demo.run(threads);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
        }
        demo.shutdown();
    }

    public class ConsumerMsgTask implements Runnable {
        private KafkaStream m_stream;
        private int m_threadNumber;

        public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
            m_threadNumber = threadNumber;
            m_stream = stream;
        }

        public void run() {// KafkaStream的本质就是一个网络迭代器
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()){
                MessageAndMetadata<byte[], byte[]> mata = it.next();
                System.out.println(String.format("[Topic=%s,Partition=%s,Offset=%s,Thread=%s] ",mata.topic(),mata.partition(),mata.offset(),m_threadNumber)+ new String(mata.message()));
            }
            System.out.println("Shutting down Thread: " + m_threadNumber);
        }
    }
}
