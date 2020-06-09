package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.Objects;

import static java.lang.Long.valueOf;

public class UserInfluence implements Comparable, Serializable {

  protected String screen_name;
  protected Long nb_followers;
  protected Long nb_tweets;
  protected Long nb_rt;
  protected float indice_influence;
  protected float rt_moyen;


  public UserInfluence() {
  }

  public UserInfluence(GenericRecord user, UserTweetRetweetCount userTweetRetweetCount) {
    this();
    this.screen_name = user.get("ScreenName").toString();
    this.nb_followers = valueOf((int) user.get("FollowersCount"));
    this.nb_rt = userTweetRetweetCount.getNb_rt();
    this.nb_tweets = userTweetRetweetCount.getNb_tweets();
    this.calculateInfluence();
    this.calculateRTMoyen();
  }

  public void calculateInfluence() {
    if (valueOf(0) != (nb_tweets * nb_followers)) {
      indice_influence = (float) nb_rt * 1000000 / (nb_tweets * nb_followers);
    }
  }

  public void calculateRTMoyen() {
    if (valueOf(0) != nb_tweets) {
      rt_moyen = (float) nb_rt / nb_tweets;
    }
  }

  @Override public int compareTo(Object o) {
    return this.indice_influence <= ((UserInfluence) o).indice_influence ? 1 : -1;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserInfluence userInfluence = (UserInfluence) o;
    return screen_name.equals(userInfluence.screen_name);
  }

  @Override public int hashCode() {
    return Objects.hash(screen_name, nb_followers, nb_tweets, nb_rt, indice_influence, rt_moyen);
  }

  @JsonCreator public UserInfluence(
      @JsonProperty("screen_name") String screen_name,
      @JsonProperty("nb_followers") Long nb_followers,
      @JsonProperty("nb_tweets") Long nb_tweets,
      @JsonProperty("nb_rt") Long nb_rt,
      @JsonProperty("indice_influence") Float indice_influence,
      @JsonProperty("rt_moyen") Long rt_moyen) {
    this.screen_name = screen_name;
    this.nb_followers = nb_followers;
    this.nb_tweets = nb_tweets;
    this.nb_rt = nb_rt;
    this.indice_influence = indice_influence;
    this.rt_moyen = rt_moyen;
  }

  @JsonProperty("screen_name")
  public String getScreen_name() {
    return screen_name;
  }

  @JsonProperty("screen_name")
  public void setScreen_name(String screen_name) {
    this.screen_name = screen_name;
  }

  @JsonProperty("nb_followers")
  public Long getNb_followers() {
    return nb_followers;
  }

  @JsonProperty("nb_followers")
  public void setNb_followers(Long nb_followers) {
    this.nb_followers = nb_followers;
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

  @JsonProperty("indice_influence")
  public float getIndice_influence() {
    return indice_influence;
  }

  @JsonProperty("indice_influence")
  public void setIndice_influence(float indice_influence) {
    this.indice_influence = indice_influence;
  }

  @JsonProperty("rt_moyen")
  public float getRt_moyen() {
    return rt_moyen;
  }

  @JsonProperty("rt_moyen")
  public void setRt_moyen(float rt_moyen) {
    this.rt_moyen = rt_moyen;
  }

  @Override public String toString() {
    return "UserInfluence{" +
        "screen_name='" + screen_name + '\'' +
        ", nb_followers=" + nb_followers +
        ", nb_tweets=" + nb_tweets +
        ", nb_rt=" + nb_rt +
        ", indice_influence=" + indice_influence +
        ", rt_moyen=" + rt_moyen +
        '}';
  }
}
