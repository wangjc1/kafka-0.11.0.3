package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * 模拟协调者在不同时间处理不同消费者的“加入组请求“
 * @author: admin
 * 2018/10/18
 */
public class KafkaConsumerRebalanceTest {
    public static void main(String[] args) throws Exception{
        //String topic = KafkaProperties.TOPIC;
        String topic = "topic";
       /* Producer producerThread = new Producer(topic, false);
        producerThread.start();*/

        KafkaSubscribeConsumer consumerThread = new KafkaSubscribeConsumer("C1", topic);
        consumerThread.start();

        Thread.sleep(2000);

        KafkaSubscribeConsumer consumerThread2 = new KafkaSubscribeConsumer("C2", topic);
        consumerThread2.start();

        Thread.sleep(2000);

        KafkaSubscribeConsumer consumerThread3 = new KafkaSubscribeConsumer("C3", topic);
        //consumerThread3.subscribe(topic);
        consumerThread3.start();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.out.println("KafkaProducerConsumerDemo shutdown hook executed...");
            }
        });
    }
}

class KafkaSubscribeConsumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private String topic;
    private String name;

    public KafkaSubscribeConsumer(String name, String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.name = name;
        this.topic = topic;
    }

    // 没有放在doWork中,因为doWork方法会被循环调用,而订阅方法只需要执行一次
    public void subscribe(String topic){
        this.topic = topic;
        consumer.subscribe(Collections.singletonList(this.topic), new RebalanceNotifyListener(this.name));
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic), new RebalanceNotifyListener(this.name));

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(this.name +
                    " 接收消息:("+record.key()+","+record.value()+"), " +
                    " 分区:" + record.partition() + ", " +
                    " 偏移量:" + record.offset());
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}

class RebalanceNotifyListener implements ConsumerRebalanceListener {
    private String consumerName;

    public RebalanceNotifyListener(String name) {
        this.consumerName = name;
    }

    /**
     * 重新分配前客户端的被分配的分区
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println(this.consumerName + " onPartitionsRevoked:"+collection);
    }

    /**
     * 重新分配后客户端的被分配的分区
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println(this.consumerName + " onPartitionsAssigned:"+collection);
    }
}
