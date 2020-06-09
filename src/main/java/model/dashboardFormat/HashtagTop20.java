package model.dashboardFormat;

import model.entityCount.HashtagCount;

public class HashtagTop20 extends TopNFormat<HashtagCount>
{
    public final static int size = 20;
    public final static String label = "hashtag-count";

    public HashtagTop20()
    {
        super(label,size);
    }
}
