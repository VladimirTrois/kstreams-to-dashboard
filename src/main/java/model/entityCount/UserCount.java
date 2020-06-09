package model.entityCount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UserCount extends EntityCount
{
    final String entityName = "user_name";

    public UserCount()
    {
        super();
    }

    @JsonCreator public UserCount(@JsonProperty(entityName) String entity, @JsonProperty("count") Long count)
    {
        super(entity,count);
    }

    @JsonProperty(entityName) //This is needed to have correct data dashboard names
    public String getEntity()
    {
        return entity;
    }
}
