package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SentimentCount implements Serializable {

  protected Long nb_tweets;
  protected Long nb_rt;

  protected Map<Long, Integer> retweet_ids;
  protected Set<Long> tweet_ids;


  public SentimentCount() {
    this.nb_tweets = Long.valueOf(0);
    this.nb_rt = Long.valueOf(0);
    this.retweet_ids = new HashMap<>();
    this.tweet_ids = new HashSet<>();
  }

  public SentimentCount addTweetStatus(GenericRecord tweetStatus) {

    if (true == (boolean) tweetStatus.get("Retweet")) {
      //si c'est un retweet
      Long originalTweetId = (Long) ((GenericRecord) tweetStatus.get("RetweetedStatus")).get("Id");
      if (tweet_ids.contains(originalTweetId)) {
        nb_rt++;
        return this;
      }

      if (retweet_ids.containsKey(originalTweetId)) {
        retweet_ids.put(originalTweetId, retweet_ids.get(originalTweetId) + 1);
      } else {
        retweet_ids.put(originalTweetId, 1);
      }
    }
    Long tweetStatusId = (Long) tweetStatus.get("Id");
    tweet_ids.add(tweetStatusId);
    nb_tweets++;
    if (retweet_ids.containsKey(tweetStatusId)) {
      nb_rt += retweet_ids.get(tweetStatusId);
      retweet_ids.remove(tweetStatusId);
    }

    return this;
  }

  @JsonCreator public SentimentCount(
      @JsonProperty("nb_tweets") Long nb_tweets,
      @JsonProperty("nb_rt") Long nb_rt,
      @JsonProperty("retweet_ids") Map<Long, Integer> retweet_ids,
      @JsonProperty("tweet_ids") Set<Long> tweet_ids) {
    this.nb_tweets = nb_tweets;
    this.nb_rt = nb_rt;
    this.retweet_ids = retweet_ids;
    this.tweet_ids = tweet_ids;
  }


  @JsonProperty("nb_tweets")
  public Long getNb_tweets() {
    return nb_tweets;
  }

  @JsonProperty("nb_tweets")
  public void setNb_tweets(Long nb_tweets) {
    this.nb_tweets = nb_tweets;
  }

  @JsonProperty("nb_rt")
  public Long getNb_rt() {
    return nb_rt;
  }

  @JsonProperty("nb_rt")
  public void setNb_rt(Long nb_rt) {
    this.nb_rt = nb_rt;
  }

  @JsonProperty("retweet_ids")
  public Map<Long, Integer> getRetweet_ids() {
    return retweet_ids;
  }

  @JsonProperty("retweet_ids")
  public void setRetweet_ids(Map<Long, Integer> retweet_ids) {
    this.retweet_ids = retweet_ids;
  }

  @JsonProperty("tweet_ids")
  public Set<Long> getTweet_ids() {
    return tweet_ids;
  }

  @JsonProperty("tweet_ids")
  public void setTweet_ids(Set<Long> tweet_ids) {
    this.tweet_ids = tweet_ids;
  }

  @Override public String toString() {
    return "UserTweetRetweetCount{" +
        "nb_tweets=" + nb_tweets +
        ", nb_rt=" + nb_rt +
        ", retweet_ids=" + retweet_ids +
        ", tweet_ids=" + tweet_ids +
        '}';
  }
}
