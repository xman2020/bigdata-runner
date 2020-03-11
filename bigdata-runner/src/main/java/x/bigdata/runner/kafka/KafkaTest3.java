package x.bigdata.runner.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 测试两个线程同时消费同一个Topic
 * Test2、Test3分别启两个进程
 */
public class KafkaTest3 {

    public static String consumerNo = "consumer1";

    public static void main(String[] args) {
        new Consumer2().start();
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
            KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
            consumer.subscribe(Arrays.asList("testTopic3"));

            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ConsumerRecords<String, String> records = consumer.poll(10);
                consumer.commitAsync();

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("consumer2 message: " + record.value());
                }
            }
        }
    }

}
