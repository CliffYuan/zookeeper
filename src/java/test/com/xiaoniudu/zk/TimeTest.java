package com.xiaoniudu.zk;

/**
 * Created by xiaoniudu on 15-1-16.
 */
public class TimeTest {
    public static void main(String[] args) {
        int i = 5;
        while (i > 0) {
            long date = System.currentTimeMillis();
            System.out.println(date + "-" + roundToInterval(date));
            i--;
        }

    }

    private static long roundToInterval(long time) {
        int expirationInterval = 2000;
        // We give a one interval grace period
        return (time / expirationInterval + 1) * expirationInterval;
    }
}
