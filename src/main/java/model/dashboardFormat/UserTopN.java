package model.dashboardFormat;

import model.entityCount.UserCount;
import model.CountingList;

import java.util.TreeSet;

public class UserTopN extends TopNFormat<UserCount> {
  public final static String label = "user-count";
  public final static int topNSize = 20;

  public UserTopN(CountingList<UserCount> countCountingList) {
    super(label, topNSize);
    this.data = topNFromCountList(countCountingList);
  }

  public TreeSet<UserCount> topNFromCountList(CountingList<UserCount> countingList) {
    TreeSet<UserCount> sortedEntityCount = new TreeSet<>();
    countingList.getCountingList().forEach((key, value) -> {
      sortedEntityCount.add(new UserCount(key, value));
      if (sortedEntityCount.size() > topNSize) {
        sortedEntityCount.pollLast();
      }
    });
    return sortedEntityCount;
  }
}
