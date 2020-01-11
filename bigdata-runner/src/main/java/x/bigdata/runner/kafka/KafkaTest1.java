package x.bigdata.runner.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 测试两个线程同时消费同一个Topic
 */
public class KafkaTest1 {

    public static String consumerNo = "consumer1";

    public static void main(String[] args) {
        new Consumer1().start();
        new Consumer2().start();
    }

    static class Consumer1 extends Thread {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "testGroup");
            props.put("enable.auto.commit", "false");
            //props.put("auto.commit.interval.ms", "1");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            while (true) {
                if (consumerNo.equals("consumer1")) {
                    consumer.subscribe(Arrays.asList("testTopic3"));
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    continue;
                }

                ConsumerRecords<String, String> records = consumer.poll(10);
                consumer.commitAsync();

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("consumer1 message: " + record.value());

                    if (record.value().equals("consumer2")) {
                        consumerNo = "consumer2";
                        consumer.unsubscribe();
                    }
                }
            }
        }
    }

    static class Consumer2 extends Thread {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "testGroup");
            props.put("enable.auto.commit", "false");
            //props.put("auto.commit.interval.ms", "1");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            while (true) {
                if (consumerNo.equals("consumer2")) {
                    consumer.subscribe(Arrays.asList("testTopic3"));
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    continue;
                }

                ConsumerRecords<String, String> records = consumer.poll(10);
                consumer.commitAsync();

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("consumer2 message: " + record.value());

                    if (record.value().equals("consumer1")) {
                        consumerNo = "consumer1";
                        consumer.unsubscribe();
                    }
                }
            }
        }

    }

}
