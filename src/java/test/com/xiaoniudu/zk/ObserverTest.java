package com.xiaoniudu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * 2015-06-04 16:12:21,980 [myid:] - INFO  [main-SendThread(127.0.0.1:2182)         :org.apache.zookeeper.ClientCnxn         @1066] - 下一次发送ping,timeToNextPing:10000,lastSend:0,lastRecv:10008
 2015-06-04 16:12:21,980 [myid:] - INFO  [main-SendThread(127.0.0.1:2182)         :org.apache.zookeeper.ClientCnxnSocketNIO@372] - 查询是否有操作,waitTimeOut(=to):9992,lastSend:0,lastRecv:10008



 2015-06-04 16:15:34,707 [myid:] - INFO  [main-SendThread(127.0.0.1:2182)         :org.apache.zookeeper.ClientCnxn         @1066] - 下一次发送ping,timeToNextPing:6666,lastSend:0,lastRecv:6670
 2015-06-04 16:15:34,708 [myid:] - INFO  [main-SendThread(127.0.0.1:2182)         :org.apache.zookeeper.ClientCnxnSocketNIO@372] - 查询是否有操作,waitTimeOut(=to):6663,lastSend:0,lastRecv:6670


 2015-06-04 16:16:10,832 [myid:] - INFO  [main-SendThread(127.0.0.1:2182)         :org.apache.zookeeper.ClientCnxn         @1066] - 下一次发送ping,timeToNextPing:3333,lastSend:0,lastRecv:3335
 2015-06-04 16:16:10,832 [myid:] - INFO  [main-SendThread(127.0.0.1:2182)         :org.apache.zookeeper.ClientCnxnSocketNIO@372] - 查询是否有操作,waitTimeOut(=to):3331,lastSend:0,lastRecv:3335


 * 观察者测试
 * Created by xiaoniudu on 15-1-21.
 */
public class ObserverTest {

    @Test
    public void test_Observer_create() {
//172.16.10.163:2181,172.16.10.96:2181,172.16.10.66:2181
        try {
            String host = "172.16.10.163:2181";
            ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("========event；" + event);
                }
            });
            Thread.sleep(5000);

            System.out.println("已经建立连接");
//
//            String path = "/mzobserver1zk1";
//            List<ACL> a = new ArrayList<ACL>();
//
//            Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
//            ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
//            a.add(acl1);
//
//            zooKeeper.create(path, "dd".getBytes(), a, CreateMode.PERSISTENT);
//
//
//            zooKeeper.addAuthInfo("digest", "admin:admin123".getBytes());
//
//            byte[] all = zooKeeper.getData(path, new Watcher() {
//                @Override
//                public void process(WatchedEvent event) {
//                    System.out.printf("====================get event:" + event);
//                }
//            }, null);
//            System.out.printf("get数据:" + all);
//
//            zooKeeper.close();

            Thread.sleep(50000000);
        } catch (Exception e) {
            Assert.fail();
        }
    }


}
