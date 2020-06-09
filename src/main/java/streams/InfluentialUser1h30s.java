package streams;

import config.KafkaConfig;
import model.UserInfluence;
import model.UserTweetRetweetCount;
import model.dashboardFormat.UserInfluenceTop30;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import tools.JsonPOJOSerde;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class InfluentialUser1h30s extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

  public String APP_ID() {
    return "InfluentialUserTop30Verified";
  }

  protected void setTopology(StreamsBuilder builder) {
    final Duration windowSize = Duration.ofHours(1);
    final Duration windowAdvance = Duration.ofSeconds(30);
    final Duration windowGrace = Duration.ofSeconds(1);

    final JsonPOJOSerde<UserTweetRetweetCount> UserTweetRetweetCountSerde = new JsonPOJOSerde<>(UserTweetRetweetCount.class);
    final JsonPOJOSerde<UserInfluence> userInfluenceSerde = new JsonPOJOSerde<>(UserInfluence.class);
    final JsonPOJOSerde<UserInfluenceTop30> userInfluenceTop30Serde = new JsonPOJOSerde<>(UserInfluenceTop30.class);

    getIngestionTweetStream(builder)
        .filter((key, value) -> statusWithOriginalTweetUserVerified(value))
        .selectKey((key, value) -> {
          if (true == (boolean) value.get("Retweet")) {
            GenericRecord retweetStatus = (GenericRecord) value.get("RetweetedStatus");
            return (GenericRecord) retweetStatus.get("User");
          } else {
            return (GenericRecord) value.get("User");
          }
        })
        .groupByKey(Grouped.with(keyGenericAvroSerde, valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .aggregate(
            () -> new UserTweetRetweetCount(),
            (key, value, agg) -> agg.addTweetStatus(value),
            Materialized.with(keyGenericAvroSerde, UserTweetRetweetCountSerde.getSerde())
        )
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .toStream()
        .mapValues((key, value) -> new UserInfluence(key.key(), value))
        .groupBy((key, value) -> key.window().endTime().toEpochMilli(), Grouped.with(Serdes.Long(), userInfluenceSerde.getSerde()))
        .aggregate(
            () -> new UserInfluenceTop30(),
            (key, value, agg) -> (UserInfluenceTop30) agg.add(value),
            Materialized.with(Serdes.Long(), userInfluenceTop30Serde.getSerde())
        )
        .toStream()
        .mapValues((key, value) -> value.toDashboardFormatString())
        //.peek((key, value) -> System.out.println(key + " " + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()))
    ;
  }

  private boolean statusWithOriginalTweetUserVerified(GenericRecord value) {
    if (true == (boolean) value.get("Retweet")) {
      GenericRecord originalTweet = (GenericRecord) value.get("RetweetedStatus");
      return (boolean) ((GenericRecord) originalTweet.get("User")).get("Verified");
    }
    return (boolean) ((GenericRecord) value.get("User")).get("Verified");
  }
}
