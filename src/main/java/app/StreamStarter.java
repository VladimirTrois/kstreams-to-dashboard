package app;

import streams.Startable;

public abstract class StreamStarter {
  protected static Startable stream = null;

  public static void main(String[] args) {
    System.out.println("start");
    //getStream().start();
  }
}
