import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaGroupProducerApp {
    public static void main(String[] args) {

        // create a properties dictionary for the required/optional Produces config settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.30.3.30:9092,10.30.3.20:9092,10.30.3.10:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);

        long time = System.currentTimeMillis();

        try {
            int counter = 0;
            while(counter < 100) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-big-topic", "abcdefghijklmnopqrstuvwxyz");
                RecordMetadata metadata = myProducer.send(record).get();
                counter++;

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
