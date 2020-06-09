package tools;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class JsonPOJOSerde<T>
{
    protected Serde<T> serde;
    protected Class<T> type;

    public JsonPOJOSerde(Class<T> type)
    {
        this.type = type;
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("JsonPOJOClass", this.type);

        Serializer<T> jsonPOJOSerializer = new JsonPOJOSerializer<>();
        jsonPOJOSerializer.configure(serdeProps, false);

        Deserializer<T> jsonPOJODeserializer = new JsonPOJODeserializer<>();
        jsonPOJODeserializer.configure(serdeProps, false);

        serde = Serdes.serdeFrom(jsonPOJOSerializer, jsonPOJODeserializer);
    }

    public Serde<T> getSerde()
    {
        return serde;
    }

    public void setSerde(Serde<T> serde)
    {
        this.serde = serde;
    }
}
