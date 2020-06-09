package model.entityCount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public abstract class EntityCount implements Comparable, Serializable
{
    protected String entity;
    protected Long count;

    public EntityCount()
    {
    }

    @JsonCreator public EntityCount(@JsonProperty("entity") String entity, @JsonProperty("count") Long count)
    {
        this.entity = entity;
        this.count = count;
    }

    @JsonProperty("entity")
    public String getEntity()
    {
        return entity;
    }

    @JsonProperty("entity")
    public void setEntity(String entity)
    {
        this.entity = entity;
    }

    @JsonProperty("count")
    public Long getCount()
    {
        return count;
    }

    @JsonProperty("count")
    public void setCount(Long count)
    {
        this.count = count;
    }

    @Override public int compareTo(Object o)
    {
        return this.count <= ((EntityCount) o).count ? 1 : -1;
    }

    @Override public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        EntityCount entityCount = (EntityCount) o;
        return entity.equals(entityCount.entity);
    }

    @Override public int hashCode()
    {
        return Objects.hash(entity, count);
    }
}
