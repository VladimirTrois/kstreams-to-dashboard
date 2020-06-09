package model.dashboardFormat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.Windowed;

public class TweetCountGraph extends DashboardFormat
{
    protected ObjectNode[] data = new ObjectNode[1];

    public TweetCountGraph(Windowed<String> window, Long count)
    {
        super("tweet-count");

        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
        this.data[0] = jsonNodeFactory.objectNode();
        this.data[0].put("ratio", count/5);
        this.data[0].put("timestamp", window.window().endTime().toEpochMilli());
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
