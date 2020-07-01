package model.entityCount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public abstract class EntityCount implements Comparable, Serializable {
  protected String entity;
  protected String entityName = null;
  protected Long count;

  public EntityCount() {
  }

  @JsonCreator public EntityCount(@JsonProperty("entity") String entity, @JsonProperty("count") Long count) {
    this.entity = entity;
    this.count = count;
  }

  public EntityCount(String entity, Long count, String entityName) {
    this.entity = entity;
    this.entityName = entityName;
    this.count = count;
  }

  @JsonProperty("entity")
  public String getEntity() {
    return entity;
  }

  @JsonProperty("entity")
  public void setEntity(String entity) {
    this.entity = entity;
  }

  @JsonProperty("count")
  public Long getCount() {
    return count;
  }

  @JsonProperty("count")
  public void setCount(Long count) {
    this.count = count;
  }

  @Override public int compareTo(Object o) {
    return this.count <= ((EntityCount) o).count ? 1 : -1;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityCount entityCount = (EntityCount) o;
    return entity.equals(entityCount.entity);
  }

  @Override public int hashCode() {
    return Objects.hash(entity, count);
  }

  public EntityCount add() {
    count = count + 1;
    return this;
  }

  @Override public String toString() {
    return "{" + entityName + "=" + entity + "," + "count=" + count + "}";
  }
}
