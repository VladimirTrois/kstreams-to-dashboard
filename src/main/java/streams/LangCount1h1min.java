package streams;

import config.KafkaConfig;
import model.entityCount.LangCount;
import model.dashboardFormat.LangTop10;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import tools.JsonPOJOSerde;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class LangCount1h1min extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

  public String APP_ID() {
    return "LangCount1h1min";
  }

  protected void setTopology(StreamsBuilder builder) {
    final Duration windowSize = Duration.ofHours(1);
    final Duration windowAdvance = Duration.ofMinutes(1);
    final Duration windowGrace = Duration.ofMillis(500);

    final JsonPOJOSerde<LangCount> langCountSerde = new JsonPOJOSerde<>(LangCount.class);
    final JsonPOJOSerde<LangTop10> langTop10Serde = new JsonPOJOSerde<>(LangTop10.class);

    getIngestionTweetStream(builder)
        .filter((key, value) -> false == value.get("Lang").toString().equals("und")) //Filter out tweet with undefined language
        .groupBy((key, value) -> value.get("Lang").toString(), Grouped.with(Serdes.String(), valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
        .count()
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .toStream()
        .mapValues((key, value) -> new LangCount(key.key(), value))
        .groupBy((key, value) -> key.window().endTime().toEpochMilli(), Grouped.with(Serdes.Long(), langCountSerde.getSerde()))
        .aggregate(
            () -> new LangTop10(),
            (key, value, agg) -> (LangTop10) agg.add(value),
            Materialized.with(Serdes.Long(), langTop10Serde.getSerde())
        )
        .toStream()
        .mapValues(value -> value.toDashboardFormatString())
        //.peek((key, value) -> System.out.println(key + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()))
    ;
  }
}
