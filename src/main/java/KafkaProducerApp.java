import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerApp {
    public static void main(String[] args) {

        // create a properties dictionary for the required/optional Produces config settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.30.3.30:9092,10.30.3.20:9092,10.30.3.10:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);

        long time = System.currentTimeMillis();

        try {
            for(int i = 0; i < 150; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                        "my_topic",
                        Integer.toBinaryString(i),
                        "My message " + Integer.toString(i)
                );

                RecordMetadata metadata = myProducer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }
    }
}
