package com.xiaoniudu.zk;

import org.apache.zookeeper.*;

/**
 * Created by xiaoniudu on 15-1-12.
 */
public class ZKTest {
    public static void main(String[] args) throws Exception {
        //create();
        createEp();
    }


    public static void create() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2182", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("dddcccc");
            }
        });

        zooKeeper.create("/mztest11", "zk001data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Thread.sleep(100);
    }

    public static void createEp() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2183", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("dddcccc");
            }
        });

        zooKeeper.create("/mztest14", "zk001data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(100000);
    }
}
