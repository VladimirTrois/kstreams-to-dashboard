package model.entityCount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LangCount extends EntityCount
{
    final String entityName = "lang";

    public LangCount()
    {
        super();
    }

    @JsonCreator public LangCount(@JsonProperty(entityName) String entity, @JsonProperty("count") Long count)
    {
        super(entity,count);
    }

    @JsonProperty(entityName) //This is needed to have correct data dashboard names
    public String getEntity()
    {
        return entity;
    }
}
