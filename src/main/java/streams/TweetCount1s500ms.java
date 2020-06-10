package streams;

import model.dashboardFormat.TweetCountGauge;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import config.KafkaConfig;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class TweetCount1s500ms extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  private static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

  public String APP_ID() {
    return "TweetCount1s500ms";
  }

  protected void setTopology(StreamsBuilder builder) {

    Duration windowSize = Duration.ofSeconds(1);
    Duration windowAdvance = Duration.ofMillis(500);
    Duration windowGrace = Duration.ofMillis(0);

    //Creating topology
    getIngestionTweetStream(builder)
        .groupBy((key, value) -> "tweets", Grouped.with(Serdes.String(), valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .count(Materialized.as("tweet_count_store"))
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .mapValues((key, value) -> (new TweetCountGauge(key, value)).toDashboardFormatString(), Named.as("to_dashboard_format"))
        .toStream()
        //.peek((key, value) -> System.out.println(key + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(windowedStringSerde, Serdes.String()))
    ;
  }
}
