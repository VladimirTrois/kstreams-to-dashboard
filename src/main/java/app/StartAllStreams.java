package app;

import model.UserTweetRetweetCount;
import model.dashboardFormat.UserInfluenceTop30;
import tools.JsonPOJOSerde;

import java.util.concurrent.TimeUnit;

public class StartAllStreams
{
    public static void main(String[] args)
    {
        try
        {
            TweetCount1s500msStart.main(args);
            TimeUnit.SECONDS.sleep(5);
            TweetCount5sStart.main(args);
            TimeUnit.SECONDS.sleep(5);
            LangCount1h1minStart.main(args);
            TimeUnit.SECONDS.sleep(5);
            HashtagCount10min10sStart.main(args);
            TimeUnit.SECONDS.sleep(5);
            UserCount10min10sStart.main(args);
            TimeUnit.SECONDS.sleep(5);
            TweetMappingStart.main(args);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
