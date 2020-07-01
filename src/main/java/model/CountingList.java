package model;

import model.entityCount.HashtagCount;

import java.util.*;

public class CountingList<T> {
  protected HashMap<String, Long> countingList;

  public CountingList() {
    this.countingList = new HashMap<>();
  }

  public CountingList add(String hashtag) {
    Long oldCount = this.countingList.get(hashtag);
    if (oldCount == null) {
      oldCount = Long.valueOf(0);
    }
    this.countingList.put(hashtag, oldCount + 1);
    return this;
  }

  public HashMap<String, Long> getCountingList() {
    return countingList;
  }

  public void setCountingList(HashMap<String, Long> countingList) {
    this.countingList = countingList;
  }
}
