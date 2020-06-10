package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import model.dashboardFormat.TweetCountGraph;

import java.time.Duration;

import config.KafkaConfig;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class TweetCount5s extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

  public String APP_ID() {
    return "TweetCount5s";
  }

  protected void setTopology(StreamsBuilder builder) {
    Duration windowSize = Duration.ofSeconds(5);
    Duration windowGrace = Duration.ofMillis(0);

    getIngestionTweetStream(builder)
        .groupBy((key, value) -> "tweets", Grouped.with(Serdes.String(), valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).grace(windowGrace))
        .count(Materialized.as("tweet_count_store"))
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .mapValues((key, value) -> (new TweetCountGraph(key, value)).toDashboardFormatString())
        .toStream()
        //.peek((key, value) -> System.out.println(key + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(windowedStringSerde, Serdes.String()))
    ;
  }
}
