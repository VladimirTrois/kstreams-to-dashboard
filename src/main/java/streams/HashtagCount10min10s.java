package streams;

import config.KafkaConfig;
import model.entityCount.HashtagCount;
import model.dashboardFormat.HashtagTop20;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import tools.JsonPOJOSerde;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class HashtagCount10min10s extends StreamsForDashboard
{
    public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
    public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

    public String APP_ID()
    {
        return "HashtagCount10min10s";
    }

    protected void setTopology(StreamsBuilder builder)
    {
        final Duration windowSize = Duration.ofMinutes(10);
        final Duration windowAdvance = Duration.ofSeconds(10);
        final Duration windowGrace = Duration.ofMillis(100);

        final JsonPOJOSerde<HashtagCount> hashtagCountSerde = new JsonPOJOSerde<>(HashtagCount.class);
        final JsonPOJOSerde<HashtagTop20> hashtagTop20Serde = new JsonPOJOSerde<>(HashtagTop20.class);

        getIngestionTweetStream(builder)
                .mapValues(value -> (GenericArray) value.get("HashtagEntities"))
                .flatMapValues(new ValueMapper<GenericArray, Iterable<GenericRecord>>()
                               {
                                   @Override public Iterable<GenericRecord> apply(GenericArray genericArray)
                                   {
                                       return genericArray;
                                   }
                               }
                )
                .mapValues(value -> value.get("Text").toString().toLowerCase())
                .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(windowSize).advanceBy(windowAdvance).grace(windowGrace))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .mapValues((key, value) -> new HashtagCount(key.key(), value)
                )
                .groupBy((key, value) -> key.window().endTime().toEpochMilli(), Grouped.with(Serdes.Long(), hashtagCountSerde.getSerde()))
                .aggregate(
                        () -> new HashtagTop20(),
                        (key, value, agg) -> (HashtagTop20) agg.add(value),
                        Materialized.with(Serdes.Long(), hashtagTop20Serde.getSerde())
                )
                .toStream()
                .mapValues(key -> key.toDashboardFormatString())
                //.peek((key, value) -> System.out.println(key + " " + value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()))
        ;
    }
}
