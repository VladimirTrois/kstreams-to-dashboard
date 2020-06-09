package app;

import model.UserTweetRetweetCount;
import model.dashboardFormat.UserInfluenceTop30;
import tools.JsonPOJOSerde;

public class StartAllStreams
{
    public static void main(String[] args)
    {
//        try
//        {
//            TweetCount1s500msStart.main(args);
//            TimeUnit.SECONDS.sleep(5);
//            TweetCount5sStart.main(args);
//            TimeUnit.SECONDS.sleep(5);
//            LangCount1h1minStart.main(args);
//            TimeUnit.SECONDS.sleep(5);
//            HashtagCount10min10sStart.main(args);
//            TimeUnit.SECONDS.sleep(5);
//            UserCount10min10sStart.main(args);
//            TimeUnit.SECONDS.sleep(5);
//            TweetMappingStart.main(args);
//        } catch (Exception e)
//        {
//            e.printStackTrace();
//        }

        final JsonPOJOSerde<UserTweetRetweetCount> userInfluenceSerde = new JsonPOJOSerde<>(UserTweetRetweetCount.class);
        final JsonPOJOSerde<UserInfluenceTop30> userInfluenceTop30Serde = new JsonPOJOSerde<>(UserInfluenceTop30.class);

        UserTweetRetweetCount userTweetRetweetCount = new UserTweetRetweetCount();
//        userTweetRetweetCount.setScreen_name("Test");
//        userTweetRetweetCount.addTweetStatus();
//        userTweetRetweetCount.addTweetStatus();
        //userInfluenceTest.addTweetStatus();
        //userInfluenceTest.addTweetStatus();

        System.out.println(userTweetRetweetCount);
        byte[] serialized = userInfluenceSerde.getSerde().serializer().serialize("lol", userTweetRetweetCount);
        System.out.println(serialized);
        UserTweetRetweetCount userInfluenceDeserialized = userInfluenceSerde.getSerde().deserializer().deserialize("lol", serialized);
        System.out.println(userInfluenceDeserialized);
        System.out.println(userInfluenceDeserialized.equals(userTweetRetweetCount));

    }
}
