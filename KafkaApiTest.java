package com.scathon.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Unit test for simple App.
 */
public class kafkaApiTest {
    private static final Properties producerPropertries = new Properties();
    private static final Properties consumerProperties = new Properties();

    static {
        //=====producerPropertries======
        producerPropertries.put("bootstrap.servers", "10.33.37.134:9092");
        producerPropertries.put("acks", "all");
        producerPropertries.put("retries", 0);
        producerPropertries.put("batch.size", 16384);
        producerPropertries.put("linger.ms", 1);
        producerPropertries.put("buffer.memory", 33554432);
        producerPropertries.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerPropertries.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //=====consumerProperties=======
        // 新版不建议使用zookeeper
        //properties.put("zookeeper.connect", "node-2:2181,node-3:2181,node-4:2181");
        consumerProperties.put("bootstrap.servers", "10.33.37.134:9092");
        consumerProperties.put("group.id", "test-consumer-group");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("session.timeout.ms", "30000");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Test
    public void producerWithMultiThread() throws InterruptedException {
        int threadCount = 5;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerPropertries);
                for (int msgIndex = 0; msgIndex < 10; msgIndex++) {
                    String message = Thread.currentThread().getName() + "-message-" + msgIndex;
                    producer.send(new ProducerRecord<>("test-01", message), (metadata, exception) -> {
                        System.out.println(Thread.currentThread().getName() + "-" + message + "-succ");
                    });
                }
                System.out.println("==================");
                countDownLatch.countDown();
            });
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        countDownLatch.await();
    }

    @Test
    public void producerApiTest() {
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(producerPropertries);
            for (int i = 0; i < 100; i++) {
                String msg = "message" + i;
                producer.send(new ProducerRecord<>("test-01", msg), ((metadata, exception) -> {
                    System.out.println("数据发送完毕。。。");
                    System.out.println(metadata);
                }));
                System.out.println("send:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    @Test
    public void consumerRepartitonListener() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        class HandlerBalancer implements ConsumerRebalanceListener {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Last partitions in rebalance Committing current offsets: " + currentOffsets);
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        }
        consumer.subscribe(Arrays.asList("test-01"), new HandlerBalancer());
        consumer.seekToBeginning();
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                consumerRecords.forEach(record -> {
                    System.out.printf("topic = %s, partitions = %s, offset = %d, customer = %s, value = %s \n"
                            , record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                });
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                System.out.println("close consumer and we are done");
            }
        }
    }

    @Test
    public void consumerApiTest() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList("SNAP_IMAGE_INFO_TOPIC"));
        consumer.seekToBeginning();
        while (true) {
            // poll()方法的参数设置为0意味着当没有数据返回的时候立马返回，如果设置为100，那么如果没有数据返回的时候，
            // 消费者将会等待100ms返回。
            ConsumerRecords<String, String> records = consumer.poll(0);
            records.forEach(record -> System.out.printf("offset = %d ,value = %s\n", record.offset(), record.value()));
        }
    }


    @Test
    public void consumerOldStreamApi() {
        ConsumerConnector consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerProperties));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("test-01", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> kafkaStreams = messageStreams.get("test-01").get(0);
        ConsumerIterator<byte[], byte[]> iterator = kafkaStreams.iterator();
        while (iterator.hasNext()) {
            System.out.println(new String(iterator.next().message()));
        }
    }
}
