package com.xiaoniudu.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class WatchTest {

    public static void main(String[] args)throws Exception{
        ZooKeeper zooKeeper=new ZooKeeper("127.0.0.1:2181",5000,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("ddd");
            }
        });
        zooKeeper.exists("/mztest3",new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(System.currentTimeMillis());
            }
        });

        Thread.sleep(1000000);


    }
}
