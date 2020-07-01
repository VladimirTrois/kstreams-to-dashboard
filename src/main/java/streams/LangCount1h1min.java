package streams;

import config.KafkaConfig;
import model.dashboardFormat.LangTopN;
import model.CountingList;
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

public class LangCount1h1min extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;
  final static Logger logger = LoggerFactory.getLogger(LangCount1h1min.class.getName());

  public String APP_ID() {
    return "LangCount1h1min";
  }

  protected void setTopology(StreamsBuilder builder) {
    final Duration windowSize = Duration.ofHours(1);
    final Duration windowAdvance = Duration.ofMinutes(1);
    final Duration windowGrace = Duration.ofMillis(500);

    final JsonPOJOSerde<CountingList> countingListSerde = new JsonPOJOSerde<>(CountingList.class);

    getIngestionTweetStream(builder)
        .filter((key, value) -> false == value.get("Lang").toString().equals("und"), Named.as("filter_out_undefined_language"))
        .mapValues((key, value) -> value.get("Lang").toString())
        .groupBy((key, value) -> "all", Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .aggregate(
            () -> new CountingList(),
            (key, value, agg) -> agg.add(value),
            Materialized.as("counting_list").with(Serdes.String(), countingListSerde.getSerde()))
        .suppress(Suppressed.untilWindowCloses(unbounded()).withName("window_suppress"))
        .toStream()
        .mapValues((key, value) -> new LangTopN(value))
        .mapValues((key, value) -> value.toDashboardFormatString())
        //.peek((key, value) -> logger.info(LocalDateTime.ofInstant(key.window().endTime(), ZoneOffset.UTC) + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(windowedStringSerde, Serdes.String()))
    ;
  }
}
