package com.xiaoniudu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoniudu on 15-1-20.
 */
public class WatcherTest {

    /**
     * 同一个节点即注册数据变更，又注册节点是否存在事件，看执行顺序
     */
    @Deprecated
    @Test
    public void test_watherGetAndExists() throws Exception {


        String host = "127.0.0.1:2181";
        ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；" + event);
            }
        });
        Thread.sleep(5000);

        System.out.println("aaaaaaaaaaa");

        String path = "/mzteststo3";


        byte[] all = zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("get event:" + event);
            }
        }, null);
        System.out.printf("all:" + all);

        Stat stat = zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("exists event:" + event);
            }
        });

        System.out.printf("stat:" + stat);

        zooKeeper.create(path,"dd".getBytes(),null, CreateMode.PERSISTENT);

        Thread.sleep(5000);
    }

    /**
     * 同一个节点即注册数据变更，又注册节点是否存在事件，
     *
     * 当更新数据时，两个watch都会执行
     *
     */
    @Test
    public void test_watherGetAndExists_setData() throws Exception {


        String host = "127.0.0.1:2181";
        ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；" + event);
            }
        });
        Thread.sleep(5000);

        System.out.println("aaaaaaaaaaa");

        String path = "/mzexistsgetWatch6";
        List<ACL> a=new ArrayList<ACL>();

        Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        a.add(acl1);

        zooKeeper.create(path,"dd".getBytes(),a, CreateMode.EPHEMERAL);


        zooKeeper.addAuthInfo("digest", "admin:admin123".getBytes());

        byte[] all = zooKeeper.getData(path,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("get event:================" + event);
            }
        }, null);
        System.out.printf("all:" + all);

        Stat stat = zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("exists event:=====================" + event);
            }
        });

        System.out.printf("stat:" + stat);

        System.out.printf("setData----------------------------------");
        stat=zooKeeper.setData(path,"ddd".getBytes(),stat.getVersion());
        System.out.printf("stat:" + stat);

        Thread.sleep(5000);
    }

    /**
     * 同一个节点即注册数据变更，又注册节点是否存在事件，
     *
     * 删除节点时，两个watch都会执行
     *
     */
    @Test
    public void test_watherGetAndExists_deleteDataNote() throws Exception {


        String host = "127.0.0.1:2181";
        ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；" + event);
            }
        });
        Thread.sleep(5000);

        System.out.println("aaaaaaaaaaa");

        String path = "/mzexistsgetWatch6";
        List<ACL> a=new ArrayList<ACL>();

        Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        a.add(acl1);

        zooKeeper.create(path,"dd".getBytes(),a, CreateMode.EPHEMERAL);


        zooKeeper.addAuthInfo("digest", "admin:admin123".getBytes());

        byte[] all = zooKeeper.getData(path,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("====================get event:" + event);
            }
        }, null);
        System.out.printf("all:" + all);

        Stat stat = zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("====================exists event:" + event);
            }
        });

        System.out.printf("stat:" + stat);

        System.out.printf("setData----------------------------------");
        zooKeeper.delete(path,stat.getVersion());

        Thread.sleep(5000);
    }

    /**
     * 同一个节点即注册数据变更，又注册节点是否存在事件，
     *
     * 关闭连接时，两个watch都会执行
     *
     */
    @Test
    public void test_watherGetAndExists_Close_EPHEMERAL() throws Exception {


        String host = "127.0.0.1:2181";
        ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("event；" + event);
            }
        });
        Thread.sleep(5000);

        System.out.println("aaaaaaaaaaa");

        String path = "/mzexistsgetWatch00";
        List<ACL> a=new ArrayList<ACL>();

        Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        a.add(acl1);

        zooKeeper.create(path,"dd".getBytes(),a, CreateMode.EPHEMERAL);


        zooKeeper.addAuthInfo("digest", "admin:admin123".getBytes());

        byte[] all = zooKeeper.getData(path,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("====================get event:" + event);
            }
        }, null);
        System.out.printf("all:" + all);

        Stat stat = zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("====================exists event:" + event);
            }
        });

        System.out.printf("stat:" + stat);

        System.out.printf("setData----------------------------------");
        zooKeeper.close();

        Thread.sleep(5000);
    }

    /**
     * 同一个节点即注册数据变更，又注册节点是否存在事件，
     *
     * 关闭连接时，都不会触发
     *
     */
    @Test
    public void test_watherGetAndExists_Close_PERSISTENT() throws Exception {


        String host = "127.0.0.1:2181";
        ZooKeeper zooKeeper = new ZooKeeper(host, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("========event；" + event);
            }
        });
        Thread.sleep(5000);

        System.out.println("aaaaaaaaaaa");

        String path = "/mzexistsgetWatch6";
        List<ACL> a=new ArrayList<ACL>();

        Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin123"));
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        a.add(acl1);

        zooKeeper.create(path,"dd".getBytes(),a, CreateMode.PERSISTENT);


        zooKeeper.addAuthInfo("digest", "admin:admin123".getBytes());

        byte[] all = zooKeeper.getData(path,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("====================get event:" + event);
            }
        }, null);
        System.out.printf("all:" + all);

        Stat stat = zooKeeper.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.printf("====================exists event:" + event);
            }
        });

        System.out.printf("stat:" + stat);

        System.out.printf("setData----------------------------------");
        zooKeeper.close();

        Thread.sleep(5000);
    }
}
