package model.dashboardFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.Windowed;

public class SentimentAnalysis extends DashboardFormat {
  protected ObjectNode[] data = new ObjectNode[1];

  public SentimentAnalysis() {
    super("sentiment-analysis");

    JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    this.data[0] = jsonNodeFactory.objectNode();
    this.data[0].put("positive", 0);
    this.data[0].put("negative", 0);
  }

  public SentimentAnalysis addSentiment(String sentiment) {
    int count = this.data[0].get(sentiment).intValue();
    this.data[0].put(sentiment, count + 1);

    return this;
  }

  public void setTimestamp(Long timestamp) {
    this.data[0].put("timestamp", timestamp);
  }

  public ObjectNode[] getData() {
    return data;
  }

  public void setData(ObjectNode[] data) {
    this.data = data;
  }
}
