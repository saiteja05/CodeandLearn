
import org.apache.avro.shaded.logisland.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;


import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class SimpleConsumer {


    public static void main(String[] args) throws InterruptedException{


        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        Object StringDeserializer;
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"myconsumers");
        prop.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,10000);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        prop.put("schema.registry.url","http://localhost:8081");
        KafkaConsumer<String, GenericRecord>  consumer = new KafkaConsumer(prop);

        Thread mainThread = Thread.currentThread();
//        this is the ctrl+c hpok
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run()
            {

                  consumer.wakeup();


                try {
                        mainThread.join();
                    } catch (InterruptedException ex) {
                       ex.printStackTrace();
                    }
                finally {
                    consumer.close();

                }
                }


        }
        );

        List<PartitionInfo> si = consumer.partitionsFor("test");
        Set<TopicPartition> partitions = new HashSet<>();
        for (PartitionInfo rec : si)
        {
            partitions.add(new TopicPartition(rec.topic(),rec.partition()));
            System.out.println("pringting ducks");
        }
        //consumer.assign(partitions); this is critical for manual partition assignment and needs to bec
//        checked manually , unlike subscribe.


        HashMap<TopicPartition,OffsetAndMetadata> offsets = new HashMap<TopicPartition,OffsetAndMetadata> ();
        List<PartitionInfo> p=consumer.partitionsFor("datajek-topic");
        System.out.println(p.toArray().toString());



        try {

        consumer.subscribe(Pattern.compile("datajek-.*"));
        int count =0;
        while(true)
        {

            ConsumerRecords<String,GenericRecord> records=consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, GenericRecord> record : records) {

                String topic = record.topic();
                int partition = record.partition();
                long recordOffset = record.offset();
                String key = record.key();
                GenericRecord avroRecord = record.value();
                System.out.println("topic: " + topic + "\n" +
                        "partition: " + partition + "\n" +
                        "recordOffset: " + recordOffset + "\n" +
                        "key: " + key + "\n");
                System.out.println( "brand: "+avroRecord.get("brand"));
                System.out.println("HorsePower: "+avroRecord.get("horsepower"));



// this is a blocking call and can lead to performance issues in batch processing
//                consumer.commitSync();

                    if(count%100==0)
                    {
                        TopicPartition tp = new TopicPartition(record.topic(),record.partition());
                        OffsetAndMetadata metadata = new OffsetAndMetadata(record.offset()+1 ,"none");
                        offsets.put(tp,metadata);
                        consumer.commitAsync(offsets,null);
                    }


            count++;
                    Thread.sleep(500);
            }

            consumer.commitAsync(new CommitCallback());
        }


    }
        catch (WakeupException w)
        {
            System.out.println("closing due to ctrl-c");
            w.printStackTrace();
            //do nothing
        }
        catch(Exception e)
        {
            e.printStackTrace();
//            consumer.commitSync(); // combine async and sync to make sure we make a graceful exit
        }
        finally {
            try
            {
                consumer.commitSync();
            }
            finally {
                consumer.close();

            }
            System.out.println("closed");
        }
}}


class CommitCallback implements OffsetCommitCallback
{


        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
            if (e != null)
            {
                System.out.println("offsets"+map+"failed to commit! \n");
                e.printStackTrace();
            }
        }

}

//
//public class RebalanceListenerExample {
//
//    private Map<TopicPartition, OffsetAndMetadata> currentOffsets =
//            new HashMap<>();
//
//    class RebalanceHandler implements ConsumerRebalanceListener {
//
//        KafkaConsumer<String, String> consumer;
//
//        RebalanceHandler(KafkaConsumer<String, String> consumer) {
//            this.consumer = consumer;
//        }
//
//        @Override
//        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//            // We are committing the offsets for the most recently processed records
//            // as we update the currentOffsets hashmap in the poll loop in the run()
//            // method after processing each record. This way we don't save the offset
//            // from the last poll() call which would have resulted in re-processing
//            // some of the records.
//            consumer.commitSync(currentOffsets);
//        }
//
//        @Override
//        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//
//        }
//    }
//
//    public void run() {
//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        props.put("group.id", "DataJekConsumers");
//        props.put("auto.offset.reset", "earliest");
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//
//        // Pass-in the instance of RebalanceHandler when subscribing to topics
//        consumer.subscribe(Pattern.compile("datajek-.*"), new RebalanceHandler(consumer));
//
//        try {
//            while (true) {
//                Duration oneSecond = Duration.ofMillis(1000);
//                System.out.println("Consumer polling for data...");
//                ConsumerRecords<String, String> records = consumer.poll(oneSecond);
//                for (ConsumerRecord<String, String> record : records) {
//
//                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
//
//                    // We add 1 to the current record's offset so that
//                    // we start reading from the record after the current
//                    // one in case of a rebalance.
//                    OffsetAndMetadata metadata = new OffsetAndMetadata(record.offset() + 1, "no metadata");
//                    currentOffsets.put(topicPartition, metadata);
//                }
//
//                // Async commit without callback
//                consumer.commitAsync();
//            }
//        } catch (Exception e) {
//            // log exception
//        } finally {
//
//            try {
//                // Initiate a synchronous commit since this is the last commit
//                // before the consumer exits. The synchronous commit will retry
//                // untill success or fail in case of an unrecoverable error.
//                consumer.commitSync();
//            } finally {
//                consumer.close();
//            }
//        }
//    }
//
//
//
//
}