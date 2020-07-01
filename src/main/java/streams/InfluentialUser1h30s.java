package streams;

import config.KafkaConfig;
import model.UserInfluence;
import model.UserTweetRetweetCount;
import model.dashboardFormat.UserInfluenceTop30;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.JsonPOJOSerde;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class InfluentialUser1h30s extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;
  final static Logger logger = LoggerFactory.getLogger(InfluentialUser1h30s.class.getName());

  public String APP_ID() {
    return "InfluentialUser1h30s";
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
        .selectKey((key, value) -> getOriginalTweetUser(value), Named.as("select_tweet_or_retweet_user"))
        .groupByKey(Grouped.with(keyGenericAvroSerde, valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .aggregate(
            () -> new UserTweetRetweetCount(),
            (key, value, agg) -> agg.addTweetStatus(value),
            Materialized.as("tweet_retweet_count_store").with(keyGenericAvroSerde, UserTweetRetweetCountSerde.getSerde())
        )
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .toStream()
        .mapValues((key, value) -> new UserInfluence(key.key(), value))
        .groupBy((key, value) -> key.window().endTime().toEpochMilli(), Grouped.with(Serdes.Long(), userInfluenceSerde.getSerde()))
        .aggregate(
            () -> new UserInfluenceTop30(),
            (key, value, agg) -> (UserInfluenceTop30) agg.add(value),
            Materialized.as("influential_user_top_n_store").with(Serdes.Long(), userInfluenceTop30Serde.getSerde())
        )
        .toStream()
        .mapValues((key, value) -> value.toDashboardFormatString())
        //.peek((key, value) -> logger.info(key + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()))
    ;
  }

  private GenericRecord getOriginalTweetUser(GenericRecord value) {
    GenericRecord user = (GenericRecord) value.get("User");
    if (true == (boolean) value.get("Retweet")) {
      GenericRecord retweetStatus = (GenericRecord) value.get("RetweetedStatus");
      user = (GenericRecord) retweetStatus.get("User");
    }
    return user;
  }

  private boolean statusWithOriginalTweetUserVerified(GenericRecord value) {
    if (true == (boolean) value.get("Retweet")) {
      GenericRecord originalTweet = (GenericRecord) value.get("RetweetedStatus");
      return (boolean) ((GenericRecord) originalTweet.get("User")).get("Verified");
    }
    return (boolean) ((GenericRecord) value.get("User")).get("Verified");
  }
}
