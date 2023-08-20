
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;
public class ProducerWithAvroSerializerExample {

    public static void main(String[] args) {
        // Set-up properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);

        // Set-up schema
        String key = "key1";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"Car\"," +
                "\"fields\":[{\"name\":\"brand\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);

        // Create an avro record
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("brand", "Mercedes");

        // Create a producer record from the avro record
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("datajek-topic", key, avroRecord);
        try {
            // Send the record
            producer.send(record);
        } catch (SerializationException e) {
            System.out.println("Exception while sending message " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}

