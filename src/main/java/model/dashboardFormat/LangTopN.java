package model.dashboardFormat;

import model.entityCount.LangCount;
import model.CountingList;

import java.util.TreeSet;

public class LangTopN extends TopNFormat<LangCount> {
  public final static String label = "language-count";
  public final static int topNSize = 10;

  public LangTopN(CountingList<LangCount> countCountingList) {
    super(label, topNSize);
    this.data = topNFromCountList(countCountingList);
  }

  public TreeSet<LangCount> topNFromCountList(CountingList<LangCount> countingList) {
    TreeSet<LangCount> sortedEntityCount = new TreeSet<>();
    countingList.getCountingList().forEach((key, value) -> {
      sortedEntityCount.add(new LangCount(key, value));
      if (sortedEntityCount.size() > topNSize) {
        sortedEntityCount.pollLast();
      }
    });
    return sortedEntityCount;
  }
}
