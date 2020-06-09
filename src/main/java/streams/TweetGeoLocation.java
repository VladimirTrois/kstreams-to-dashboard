package streams;

import config.KafkaConfig;
import model.dashboardFormat.TweetMap;
import org.apache.kafka.streams.StreamsBuilder;

public class TweetGeoLocation extends StreamsForDashboard
{
    public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
    public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

    public String APP_ID()
    {
        return "TweetMapping";
    }

    protected void setTopology(StreamsBuilder builder)
    {
        getIngestionTweetStream(builder)
                .filter((key,value) -> null != value.get("GeoLocation"))
                .mapValues((key,value) -> (new TweetMap(value).toDashboardFormatString()))
                //.peek((key, value) -> System.out.println(key + " " + value))
                .to(OUTPUT_TOPIC)
        ;
    }
}
