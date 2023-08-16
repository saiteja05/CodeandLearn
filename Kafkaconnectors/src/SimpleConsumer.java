import org.apache.kafka.clients.consumer.*;
import org.junit.platform.commons.function.Try;


import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class SimpleConsumer {


    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        Object StringDeserializer;
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"myconsumers");
        prop.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,10000);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        KafkaConsumer consumer = new KafkaConsumer(prop);
        try {

        consumer.subscribe(Pattern.compile("datajek-.*"));

        while(true)
        {

            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {

                String topic = record.topic();
                int partition = record.partition();
                long recordOffset = record.offset();
                String key = record.key();
                String value = record.value();

                System.out.println("topic: %s" + topic + "\n" +
                        "partition: %s" + partition + "\n" +
                        "recordOffset: %s" + recordOffset + "\n" +
                        "key: %s" + key + "\n" +
                        "value: %s" + value);
// this is a blocking call and can lead to performance issues in batch processing
//                consumer.commitSync();
            }
        }


    }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally {

            consumer.close();
            System.out.println("closed");
        }
}}
