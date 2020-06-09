package model.dashboardFormat;

import model.entityCount.UserCount;

public class UserTop20 extends TopNFormat<UserCount>
{
    public final static int size = 20;
    public final static String label = "user-count";

    public UserTop20()
    {
        super(label,size);
    }
}
