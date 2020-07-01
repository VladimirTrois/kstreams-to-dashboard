package model.dashboardFormat;

import java.util.TreeSet;

public abstract class TopNFormat<T> extends DashboardFormat {
  public static int size;
  public TreeSet<T> data;

  public TopNFormat(String label, int size) {
    super(label);
    this.size = size;
    this.data = new TreeSet<>();
  }

  public TopNFormat add(T entityCount) {
    this.data.add(entityCount);
    if (size < this.data.size()) {
      this.data.pollLast();
    }
    return this;
  }

  public TopNFormat remove(T entityCount) {
    this.data.remove(entityCount);
    return this;
  }

  public TreeSet<T> getData() {
    return data;
  }

  public void setData(TreeSet<T> data) {
    this.data = data;
  }

  @Override public String toString() {
    return "{label=" + label + ", data=" + data.toString() + "}";
  }
}
