package app;

import config.KafkaConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Verify
{

    public static void main(String[] args)
    {
        Properties props = getConsumerProperties();
        KafkaConsumer<String, GenericRecord> kafkaConsumer = new KafkaConsumer<>(props);

        TopicPartition partition = new TopicPartition("twitter_ingestion",0);
        Collection<TopicPartition> topics = Collections.singleton(partition);
        kafkaConsumer.assign(topics);

        while (true)
        {
            kafkaConsumer.seekToBeginning(topics);
            System.out.println("Restart");
            ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            int i = 0;
            for (ConsumerRecord<String, GenericRecord> record : records)
            {
                GenericRecord genericRecord = record.value();

                //ConsumerRecord<Windowed<String>, Long> test= record;
                System.out.println(record.offset() + " " + genericRecord.get("CreatedAt") + " " + genericRecord.get("Id"));
                //System.out.println((record.key().window().endTime().toEpochMilli() + " " + windowTime.getTime()));

                if (i > 3)
                {
                    break;
                }
                i++;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //kafkaConsumer.commitAsync();
        }

    }

    private static Properties getConsumerProperties()
    {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter-consumer");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.getKafkaServerUrl());
        props.setProperty(ConsumerConfig.CLIENT_RACK_CONFIG, "1");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", KafkaConfig.getKafkaSchemaUrl());
        //props.setProperty("specific.avro.reader", "true");

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }
}
