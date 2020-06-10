package streams;

import config.KafkaConfig;
import model.entityCount.UserCount;
import model.dashboardFormat.UserTop20;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import tools.JsonPOJOSerde;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class UserCount10min10s extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

  public String APP_ID() {
    return "UserCount10min10s";
  }

  protected void setTopology(StreamsBuilder builder) {
    final Duration windowSize = Duration.ofMinutes(10);
    final Duration windowAdvance = Duration.ofSeconds(10);
    final Duration windowGrace = Duration.ofMillis(0);

    final JsonPOJOSerde<UserCount> userCountSerde = new JsonPOJOSerde<>(UserCount.class);
    final JsonPOJOSerde<UserTop20> userTop20Serde = new JsonPOJOSerde<>(UserTop20.class);

    getIngestionTweetStream(builder)
        .mapValues(value -> (GenericRecord) value.get("User"))
        .groupBy((key, value) -> value.get("ScreenName").toString(), Grouped.with(Serdes.String(), valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .count(Materialized.as("user_tweeting_count_store"))
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .toStream()
        .mapValues((key, value) -> new UserCount(key.key(), value))
        .groupBy((key, value) -> key.window().endTime().toEpochMilli(), Grouped.with(Serdes.Long(), userCountSerde.getSerde()))
        .aggregate(
            () -> new UserTop20(),
            (key, value, agg) -> (UserTop20) agg.add(value),
            Materialized.as("user_count_top_n_store").with(Serdes.Long(), userTop20Serde.getSerde())
        )
        .toStream()
        .mapValues(key -> key.toDashboardFormatString(), Named.as("to_dashboard_format"))
        //.peek((key, value) -> System.out.println(key + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()))
    ;
  }
}
