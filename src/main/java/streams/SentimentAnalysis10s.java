package streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import config.KafkaConfig;
import model.dashboardFormat.LangTop10;
import model.dashboardFormat.SentimentAnalysis;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import tools.JsonPOJOSerde;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class SentimentAnalysis10s extends StreamsForDashboard {
  public final static String INPUT_TOPIC = KafkaConfig.TWITTER_INGESTION_TOPIC;
  public final static String OUTPUT_TOPIC = KafkaConfig.DASHBOARD_DATA_TOPIC;
  public final static String SENTIMENT_API_URL = "http://7f3f0cc9-669b-4926-8804-a671f00867a1.westeurope.azurecontainer.io/score";

  private HttpClient client = HttpClient.newHttpClient();

  public String APP_ID() {
    return "SentimentAnalysis10sTest2";
  }

  protected void setTopology(StreamsBuilder builder) {
    final Duration windowSize = Duration.ofSeconds(10);
    final Duration windowGrace = Duration.ofMillis(500);

    final JsonPOJOSerde<SentimentAnalysis> sentimentAnalysisSerde = new JsonPOJOSerde<>(SentimentAnalysis.class);

    getIngestionTweetStream(builder)
        .filter((key, value) -> true == value.get("Lang").toString().equals("en")) //Keep onlly english tweets
        .groupBy((key, value) -> value.get("Lang").toString(), Grouped.with(Serdes.String(), valueGenericAvroSerde))
        .windowedBy(TimeWindows.of(windowSize).grace(windowGrace))
        .aggregate(
            SentimentAnalysis::new,
            (key, value, agg) -> agg.addSentiment(sentimentAnalysis(value)),
            Materialized.with(Serdes.String(), sentimentAnalysisSerde.getSerde())
        )
        .suppress(Suppressed.untilWindowCloses(unbounded()))
        .toStream()
        .mapValues((key, value) ->
                   {
                     value.setTimestamp(key.window().endTime().toEpochMilli());
                     return value.toDashboardFormatString();
                   })
        //.peek((key, value) -> System.out.println(key + " " + value))
        .to(OUTPUT_TOPIC, Produced.with(windowedStringSerde, Serdes.String()))
    ;
  }

  private String sentimentAnalysis(GenericRecord value) {
    String sentimentAnalyse = null;

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode requestJson = mapper.createObjectNode();
    ArrayNode texts = mapper.createArrayNode();
    texts.add(value.get("Text").toString());
    requestJson.set("data", texts);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(SENTIMENT_API_URL))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestJson.toString()))
        .build();
    try {
      //L'api retourn :  "\"[4 4 4]\"" , il faut parse deux fois
      HttpResponse<String> responseFromApi = client.send(request, HttpResponse.BodyHandlers.ofString());
      String responseString = mapper.readValue(responseFromApi.body(), String.class); // ""[4]""
      String response = mapper.readValue(responseString, String.class); // "[4]"
      ArrayNode res = mapper.readValue(response, ArrayNode.class); //[4]

      sentimentAnalyse = "positive";
      if (res.get(0).toString().equals("0")) {
        sentimentAnalyse = "negative";
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return sentimentAnalyse;
  }
}
