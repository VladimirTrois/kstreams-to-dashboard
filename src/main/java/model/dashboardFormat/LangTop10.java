package model.dashboardFormat;

import model.entityCount.LangCount;

public class LangTop10 extends TopNFormat<LangCount>
{
    public final static int size = 10;
    public final static String label = "language-count";

    public LangTop10()
    {
        super(label,size);
    }
}
