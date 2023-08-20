


        import org.apache.avro.generic.GenericRecord;
        import org.apache.kafka.clients.consumer.ConsumerRecord;
        import org.apache.kafka.clients.consumer.ConsumerRecords;
        import org.apache.kafka.clients.consumer.KafkaConsumer;

        import java.time.Duration;
        import java.util.Properties;
        import java.util.regex.Pattern;

public class ConsumerWithAvroSerializerExample {
    public static void main(String[] args) {

        // Set up properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "DataJekConsumers");
        props.put("auto.offset.reset", "earliest");

        // We also pass-in the URL for the schema registry
        props.put("schema.registry.url", "http://localhost:8081");

        // Create a Kafka consumer and subscribe to topic
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile("datajek-.*"));

        try {
            while (true) {
                Duration oneSecond = Duration.ofMillis(1000);

                // Since the Producer wrote GenericRecords in the topic, our consumer will
                // read GenericRecords from the topic.
                ConsumerRecords<String, GenericRecord> records = consumer.poll(oneSecond);

                for (ConsumerRecord<String, GenericRecord> record : records) {

                    // Read the fields of the GenericRecord and print them on the
                    // console.
                    System.out.println("Offset " + record.offset() + " " +
                            "Car Make :" + record.value().get("make") + " " +
                            "Car Model :" + record.value().get("model"));
                }

                // Commit the offset
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}