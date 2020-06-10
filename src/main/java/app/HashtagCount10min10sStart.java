package app;

import streams.HashtagCount10min10s;

public class HashtagCount10min10sStart extends StreamStarter {
    public static void main(String[] args)
    {
        (new HashtagCount10min10s()).start();
    }
}
