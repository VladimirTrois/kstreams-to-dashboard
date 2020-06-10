package streams;

import config.KafkaConfig;
import model.dashboardFormat.TweetMap;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;

public class TweetGeoLocation extends StreamsForDashboard
{
    public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
    public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;

    public String APP_ID()
    {
        return "TweetGeoLocation";
    }

    protected void setTopology(StreamsBuilder builder)
    {
        getIngestionTweetStream(builder)
                .filter((key,value) -> null != value.get("GeoLocation"), Named.as("filter_out_tweet_without_geolocation"))
                .mapValues((key,value) -> (new TweetMap(value).toDashboardFormatString()))
                //.peek((key, value) -> System.out.println(key + " " + value))
                .to(OUTPUT_TOPIC)
        ;
    }
}
