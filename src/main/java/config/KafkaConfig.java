package config;

public class KafkaConfig
{

    //Kafka Configuration
    public static final String KAFKA_HOST_ADDRESS = "34.77.65.196";
    private static final String KAFKA_HOST_PORT = "9093";
    private static final String KAFKA_SCHEMA_URL_PORT = "8081";

    public static String getKafkaSchemaUrl()
    {
        return "http://" + KAFKA_HOST_ADDRESS + ":" + KAFKA_SCHEMA_URL_PORT;
    }
    public static String getKafkaServerUrl()
    {
        return KAFKA_HOST_ADDRESS + ":" + KAFKA_HOST_PORT;
    }

    //Topic name
    public static final String TWITTER_INGESTION_TOPIC = "twitter_ingestion";
    public static final String DASHBOARD_DATA_TOPIC = "dashboard_data";

    //Streams Configuration
    public static final String KAFKA_STREAMS_STATE_DIR = "/tmp/kafka/kafka-streams";

}
