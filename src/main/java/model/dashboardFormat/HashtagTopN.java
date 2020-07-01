package model.dashboardFormat;

import model.entityCount.HashtagCount;
import model.CountingList;

import java.util.TreeSet;

public class HashtagTopN extends TopNFormat<HashtagCount> {
  public final static String labelForDashboard = "hashtag-count";
  public final static int topNSize = 20;

  public HashtagTopN(CountingList<HashtagCount> countingList) {
    super(labelForDashboard, topNSize);
    this.data = topNFromCountList(countingList);
  }

  public TreeSet<HashtagCount> topNFromCountList(CountingList<HashtagCount> countingList) {
    TreeSet<HashtagCount> sortedEntityCount = new TreeSet<>();
    countingList.getCountingList().forEach((key, value) -> {
      sortedEntityCount.add(new HashtagCount(key, value));
      if (sortedEntityCount.size() > topNSize) {
        sortedEntityCount.pollLast();
      }
    });
    return sortedEntityCount;
  }
}
