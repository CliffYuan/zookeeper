package com.xiaoniudu.zk;

import org.apache.zookeeper.*;

/**
 * Created by xiaoniudu on 15-1-12.
 */
public class ZKTest {
    public static void main(String[] args) throws Exception{
        ZooKeeper zooKeeper=new ZooKeeper("127.0.0.1:2181",5000,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("dddcccc");
            }
        });

        zooKeeper.create("/mztest7", "zk001data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Thread.sleep(100);
    }
}
