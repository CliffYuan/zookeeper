package com.xiaoniudu.zk;

import org.apache.zookeeper.*;
import org.junit.Test;

/**
 * Created by xiaoniudu on 15-1-16.
 */

public class SessionTimeOutTest {

    /**
     * 测试follower接收不到client的请求，服务端sessionTimeOut
     */
    @Test
    public void testServerSessionTimeOut() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；"+event);
            }
        });

        String path="/mzteststo1";
        zooKeeper.create(path, "zk001data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Thread.sleep(50000);

        zooKeeper.getData(path,false,null);

        Thread.sleep(50000);
    }

    /**
     * 测试报连接异常
     *
     * @throws Exception
     */
    @Test
    public void testServerSessionTimeOut_ServerIsNotExit() throws Exception {
        //String host="192.168.17.4:2188";//java.net.NoRouteToHostException: 没有到主机的路由
        //String host="172.16.132.114:2181";//java.net.ConnectException: 拒绝连接   ------------本机
        String host="172.16.3.29:2181";
        ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；"+event);
            }
        });
        Thread.sleep(50000);

        System.out.println("aaaaaaaaaaa");

//        String path="/mzteststo1";
//        zooKeeper.create(path, "zk001data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        Thread.sleep(500000);
//
//        zooKeeper.getData(path,false,null);

        Thread.sleep(500000);
    }


}
