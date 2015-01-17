package com.xiaoniudu.zk;

import org.apache.zookeeper.*;
import org.junit.Test;

/**
 * Created by xiaoniudu on 15-1-17.
 */
public class SessionExprieTest {


    /**
     * 测试Leader判断session过期
     * 测试方法：客户端连接的那个follower不进行更新session,注释touchSession()方法
     */
    @Test
    public void testSessionExprie() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2183", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；"+event);
            }
        });

        Thread.sleep(100000);

        System.out.printf("end");
    }
}
