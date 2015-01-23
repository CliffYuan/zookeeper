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
 * 镜像和日志测试
 *
 * Created by xiaoniudu on 15-1-22.
 */
public class SnapTxnLogTest {

    /**
     * 测试两次创建，日志情况
     */
    @Test
    public void test_Create2(){
        try {
            String host = "127.0.0.1:2181";
            ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("========event；" + event);
                }
            });
            Thread.sleep(5000);

            System.out.println("已经建立连接");

            String path = "/mzobserver1";
            List<ACL> a = new ArrayList<ACL>();

            Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
            ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
            a.add(acl1);

            zooKeeper.create(path, "dd".getBytes(), a, CreateMode.PERSISTENT);

            zooKeeper.create(path, "dd".getBytes(), a, CreateMode.PERSISTENT);


            zooKeeper.close();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    /**
     * 测试两次创建，
     */
    @Test
    public void test_Create(){
        try {
            String host = "127.0.0.1:2181";
            ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("========event；" + event);
                }
            });
            Thread.sleep(5000);

            System.out.println("已经建立连接");

            String path = "/mzobserver123uuwe567kkk";
            List<ACL> a = new ArrayList<ACL>();

            Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
            ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
            a.add(acl1);

            zooKeeper.create(path, "dd".getBytes(), a, CreateMode.PERSISTENT);


            Thread.sleep(100000000);


            zooKeeper.close();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }
}
