/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * 1. 新版Consumer的动态平衡和老板的机制不一样，老板的Consumer客户端连接信息存储在ZK中，
 *    所以可以监听其动态变化，而新版是通过HeartbeatThread不断发送心跳，在心跳请求响应回调中来确定是否需要Rebalance
 *
 */
public class KafkaNewConsumerProducer {
    public static void main(String[] args) {
       /* boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
        producerThread.start();*/

       for(int i=0;i<1;i++){
           Consumer consumerThread = new Consumer("KafkaConsumerExample_"+i,KafkaProperties.TOPIC);
           consumerThread.start();
       }
    }
}

class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final ConcurrentHashMap<TopicPartition,Long> offsetManager = new ConcurrentHashMap();

    public Consumer(String name,String topic) {
        super(name, false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumerGroup");
        // 自动提交消费完的offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 每隔多长时间提交一次offset
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //每次拉取的记录条数
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        //Session过期时间
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        //最大拉取数据间隔时间，用作初始化InitialDelayedJoin的剩余时间
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "40000");
        //心跳时间间隔
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        //从何处开始消费,latest 表示消费最新消息,earliest 表示从头开始消费,none表示抛出异常,默认latest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //序列号类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        //subscribe相当于老版本中High API,可以实现动态分配动态平衡的功能
        consumer.subscribe(Collections.singletonList(this.topic), new ConsumerRebalanceListener() {
            /**
             * 这个方法可以保存偏移量到介质中，比如DB、HDFS
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for(TopicPartition p:partitions){
                    offsetManager.put(p,consumer.position(p));
                    Utils.println("Topic=%s,Partition=%s,Postition=%s",p.topic(),p.partition()+"",consumer.position(p)+"");
                }
            }

            /**
             * 这个方法中可以从介质中把偏移量读出来
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for(TopicPartition p:partitions){
                    consumer.seek(p,offsetManager.get(p)==null?0:offsetManager.get(p));
                    Utils.println("Topic=%s,Partition=%s,Postition=%s",p.topic(),p.partition()+"",offsetManager.get(p)+"");
                }
            }
        });

        //assign相当于老版本中Low API,无动态分配再平衡功能
        //consumer.assign(Collections.singletonList(new TopicPartition(topic,leader)));
        //消费指定偏移量的记录
        //consumer.seek(new TopicPartition(topic,leader),0);

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
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

class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // batch.size是producer批量发送的基本单位，默认是16384Bytes，即16kB；
        // lingger.ms是sender线程在检查batch是否ready时候，判断有没有过期的参数，默认大小是0ms。
        // 那么producer是按照batch.size大小批量发送消息呢，还是按照linger.ms的时间间隔批量发送消息呢？
        // 这里先说结论：其实满足batch.size和ling.ms之一，producer便开始发送消息。
        props.put("batch.size", 2048);
        //0. 生产者不会等待服务端的任何应答。
        //1. 生产者会等待主副本收到一个应答后，认为生产请求完成了
        //-1. 示生产者发送生产请求后，“所有处于同步的备份副本(ISR)都向主副本发送了应答之后，生产请求才算完成
        props.put("request.required.acks", "0");

        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public static void main(String[] args) throws Exception {
        boolean isAsync = false;//args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
        producerThread.start();

        //Thread.sleep(2000);
    }


    public void run() {
        int messageNo =1;
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
            if(messageNo>5) break;
        }
    }

    private static class DemoCallBack implements Callback {

        private final long startTime;
        private final int key;
        private final String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println(
                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                                "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }


}