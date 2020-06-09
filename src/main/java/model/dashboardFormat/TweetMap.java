package model.dashboardFormat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericRecord;

public class TweetMap extends DashboardFormat
{
    public static long maximumCount = 0;
    protected ObjectNode data;

    public TweetMap(GenericRecord tweet)
    {
        super("tweet-location");

        GenericRecord geoLocation = (GenericRecord) tweet.get("GeoLocation");
        GenericRecord user = (GenericRecord) tweet.get("User");

        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

        this.data = jsonNodeFactory.objectNode();
        this.data.put("lat", geoLocation.get("Latitude").toString());
        this.data.put("lng", geoLocation.get("Longitude").toString());
        this.data.put("text", tweet.get("Text").toString());
        this.data.put("screen_name", user.get("ScreenName").toString());
        this.data.put("photo", user.get("ProfileImageURL").toString());
    }

    public ObjectNode getData()
    {
        return data;
    }

    public void setData(ObjectNode data)
    {
        this.data = data;
    }
}
