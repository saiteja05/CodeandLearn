import org.apache.avro.shaded.logisland.Schema;
import org.apache.avro.shaded.logisland.generic.GenericData;
import org.apache.avro.shaded.logisland.generic.GenericRecord;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.*;


import java.util.Properties;

public class SerializedProducer {


    public static void main(String[] args) {
        // Set-up properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG\\\\\\\\\\);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);

        // Set-up schema
        String key = "key2";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"Car\"," +
                "\"fields\":[{\"name\":\"brand\",\"type\":\"string\"}," +
                "{\"name\":\"horsepower\",\"type\":\"int\",\"default\":-1}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        int max = 15;

        // Create an avro record
//        GenericRecord avroRecord = new GenericData.Record(schema);
        try {
        while (max > 0) {
            String brand = "BMWSeries"+Math.ceil(Math.random()*max);
            int hp = (int) (100*Math.random()*max);
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("brand", brand);
            avroRecord.put("horsepower", hp);

            // Create a producer record from the avro record
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("datajek-topic", key, avroRecord);

                // Send the record
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.println(recordMetadata.toString());
                        }
                    }
                });
//                Thread.sleep(200);
                --max;
            }
        }
        catch (SerializationException e){
                System.out.println("Exception while sending message " + e.getMessage());
                e.printStackTrace();
            } finally {
                producer.flush();
                producer.close();
                max=0;
            }

    }
}


//class AsyncConfirmer implements Callback
//{
//
//    @Override
//    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//        if(e != null)
//        {
//            e.printStackTrace();
//        }
//        else
//        {
//            System.out.println(recordMetadata.toString());
//        }
//    }
//}