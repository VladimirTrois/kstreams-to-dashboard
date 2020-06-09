package model.dashboardFormat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.Windowed;

import static java.lang.Long.valueOf;

public class TweetCountGauge extends DashboardFormat
{
    public static long maximumCount = 0;
    protected ObjectNode[] data = new ObjectNode[1];

    public TweetCountGauge(Windowed<String> window, Long count)
    {
        super("tweet-count-gauge");

        maximumCount = Math.max(maximumCount, count);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
        this.data[0] = jsonNodeFactory.objectNode();
        this.data[0].put("ratio", count);
        this.data[0].put("timestamp", window.window().endTime().toEpochMilli());
        this.data[0].put("max", maximumCount);
    }

    public ObjectNode[] getData()
    {
        return data;
    }

    public void setData(ObjectNode[] data)
    {
        this.data = data;
    }
}
