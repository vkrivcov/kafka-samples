import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Use the bin/kafka-producer-perf.sh to test the consumers with messages
 * bin/kafka-producer-perf-test.sh --topic my-topic2 --num-records 50 --record-size 1 --throughput 10 --producer-props bootstrap.servers=10.30.3.30:9092,10.30.3.20:9092,10.30.3.10:9092 key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer
 * bin/kafka-producer-perf-test.sh --topic my-topic3 --num-records 50 --record-size 1 --throughput 10 --producer-props bootstrap.servers=10.30.3.30:9092,10.30.3.20:9092,10.30.3.10:9092 key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer
 */

public class KafkaConsumerAssignApp {
    public static void main(String[] args) {
        // create a properties dictionary for the required/optional Consumer config settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.30.3.30:9092,10.30.3.20:9092,10.30.3.10:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        TopicPartition myTopicPart0 = new TopicPartition("my-topic2", 0);
        TopicPartition myTopicPart2 = new TopicPartition("my-topic3", 2);

        partitions.add(myTopicPart0);
        partitions.add(myTopicPart2);

        myConsumer.assign(partitions);

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(10);
                for(ConsumerRecord<String, String> record : records) {
                    // process each record
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }
    }
}
