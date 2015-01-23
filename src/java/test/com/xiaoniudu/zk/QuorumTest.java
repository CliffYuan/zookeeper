package com.xiaoniudu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoniudu on 15-1-21.
 */
public class QuorumTest {

    @Test
    public void test_QuorumIng_Create(){
        try {
            String host = "127.0.0.1:2182";
            ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("========event；" + event);
                }
            });
            Thread.sleep(5000);

            System.out.println("已经建立连接");

            String path = "/mzquorum";
            List<ACL> a = new ArrayList<ACL>();

            Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
            ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
            a.add(acl1);

            zooKeeper.create(path, "dd".getBytes(), a, CreateMode.EPHEMERAL);


            zooKeeper.addAuthInfo("digest", "admin:admin123".getBytes());

            byte[] all = zooKeeper.getData(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.printf("====================get event:" + event);
                }
            }, null);
            System.out.printf("get数据:" + all);

            zooKeeper.close();
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
