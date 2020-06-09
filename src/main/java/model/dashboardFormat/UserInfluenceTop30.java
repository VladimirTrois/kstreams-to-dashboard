package model.dashboardFormat;

import model.UserInfluence;

public class UserInfluenceTop30 extends TopNFormat<UserInfluence>
{
    public final static int size = 30;
    public final static String label = "influencial-users";

    public UserInfluenceTop30()
    {
        super(label,size);
    }
}
