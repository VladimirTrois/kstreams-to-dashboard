package model.entityCount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class HashtagCount extends EntityCount
{
    final String entityName = "hashtag";

    public HashtagCount()
    {
        super();
    }

    @JsonCreator public HashtagCount(@JsonProperty(entityName) String entity, @JsonProperty("count") Long count)
    {
        super(entity, count);
    }

    @JsonProperty(entityName) //This is needed to have correct data dashboard names
    public String getEntity()
    {
        return entity;
    }
}
