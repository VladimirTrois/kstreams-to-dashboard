package streams;

import config.KafkaConfig;
import model.dashboardFormat.HashtagTopN;
import model.CountingList;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.JsonPOJOSerde;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class HashtagCount10min10s extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;
  final static Logger logger = LoggerFactory.getLogger(HashtagCount10min10s.class.getName());

  public String APP_ID() {
    return "HashtagCount10min10s";
  }

  protected void setTopology(StreamsBuilder builder) {
    final Duration windowSize = Duration.ofMinutes(10);
    final Duration windowAdvance = Duration.ofSeconds(10);
    final Duration windowGrace = Duration.ofMillis(100);

    final JsonPOJOSerde<CountingList> countListSerde = new JsonPOJOSerde<>(CountingList.class);

    getIngestionTweetStream(builder)
        .mapValues(value -> (GenericArray) value.get("HashtagEntities"))
        .flatMapValues(new ValueMapper<GenericArray, Iterable<GenericRecord>>() {
          @Override public Iterable<GenericRecord> apply(GenericArray genericArray) {
            return genericArray;
          }
        })
        .mapValues((key, value) -> value.get("Text").toString().toLowerCase())
        .groupBy((key, value) -> "all", Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .aggregate(
            () -> new CountingList(),
            (key, value, agg) -> agg.add(value),
            Materialized.as("counting_list").with(Serdes.String(), countListSerde.getSerde()))
        .suppress(Suppressed.untilWindowCloses(unbounded()).withName("window_suppress"))
        .toStream()
        .mapValues((key, value) -> new HashtagTopN(value))
        .mapValues((key, value) -> value.toDashboardFormatString())
        //.peek((key, value) -> logger.info(LocalDateTime.ofInstant(key.window().endTime(), ZoneOffset.UTC) + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(windowedStringSerde, Serdes.String()))
    ;
  }
}
