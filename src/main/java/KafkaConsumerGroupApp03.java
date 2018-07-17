import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerGroupApp03 {
    public static void main(String[] args) {
        // create a properties dictionary for the required/optional Consumer config settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.30.3.30:9092,10.30.3.20:9092,10.30.3.10:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test-group");

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("my-big-topic");

        myConsumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(10);
                for(ConsumerRecord<String, String> record : records) {
                    // process each record
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value().toUpperCase()));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }
    }
}
