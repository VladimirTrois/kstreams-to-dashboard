package streams;

import config.KafkaConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public abstract class StreamsForDashboard implements Startable {
  public abstract String APP_ID();

  //Serdes configuration
  final Map<String, String> serdeConfig = Collections.singletonMap(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      KafkaConfig.getKafkaSchemaUrl()
  );
  final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
  final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
  Serde<Windowed<String>> windowedStringSerde = new WindowedSerdes.TimeWindowedSerde<String>(Serdes.String());

  public void start() {
    Properties streamsConfiguration = getStreamsConfiguration();
    StreamsBuilder builder = new StreamsBuilder();
    setTopology(builder);
    KafkaStreams mainStream = new KafkaStreams(builder.build(), streamsConfiguration);
    mainStream.cleanUp();
    mainStream.start();
    Runtime.getRuntime().addShutdownHook(new Thread(mainStream::close));
  }

  protected abstract void setTopology(StreamsBuilder builder);

  protected Properties getStreamsConfiguration() {
    Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID());
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, KafkaConfig.KAFKA_STREAMS_STATE_DIR);
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100); // 30000 milliseconds (at-least-once) / 100 milliseconds (exactly-once)
    streamsConfiguration.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 600000);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.getKafkaServerUrl());
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConfig.getKafkaSchemaUrl());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    streamsConfiguration.put(StreamsConfig.TOPIC_PREFIX + TopicConfig.RETENTION_MS_CONFIG, 600000);
    //streamsConfiguration.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass().getName());
    //streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
    //streamsConfiguration.put(StreamsConfig.TOPIC_PREFIX + TopicConfig.RETENTION_MS_CONFIG, 600000);

    return streamsConfiguration;
  }

  public KStream<GenericRecord, GenericRecord> getIngestionTweetStream(StreamsBuilder builder) {
    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

    return builder.stream(KafkaConfig.TWITTER_INGESTION_TOPIC, Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde));
  }
}
