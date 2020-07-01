package app;

import streams.*;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class StartAllStreams {

  public static void main(String[] args) {

    ArrayList<Startable> streams = new ArrayList<>();
    streams.add(new TweetGeoLocation());
    streams.add(new LangCount1h1min());
    streams.add(new InfluentialUser1h30s());
    streams.add(new HashtagCount10min10s());
    streams.add(new SentimentAnalysis10s());
    streams.add(new UserCount10min10s());
    streams.add(new TweetCount1s500ms());
    streams.add(new TweetCount5s());

    streams.forEach(stream -> startAndSleep(stream));

  }

  public static void startAndSleep(Startable startable) {
    try {
      startable.start();
      TimeUnit.SECONDS.sleep(5);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

