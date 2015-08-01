/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dump a snapshot file to stdout.
 * 镜像文件dump
 */
public class SnapshotFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotFormatter.class);

    /**
     * USAGE: SnapshotFormatter snapshot_file
     */
    public static void main(String[] args) throws Exception {
//        if (args.length != 1) {
//            System.err.println("USAGE: SnapshotFormatter snapshot_file");
//            System.exit(2);
//        }
      //  String filePath="/home/xiaoniudu/share-doc2win7/zk/snapshot.b04dbc67d";
        //String filePath="/home/xiaoniudu/share-doc2win7/zk/snapshot.b04daf7cc";

      //  String filePath="/home/xiaoniudu/share-doc2win7/zk/132/snapshot.b04daff15";

    //    String filePath="/home/xiaoniudu/share-doc2win7/zk/132/snapshot.b04dc3f5c";

      //  String filePath="/home/xiaoniudu/share-doc2win7/zk/25/snapshot.b04dc3f5c";

      //  String filePath="/home/xiaoniudu/share-doc2win7/zk/27/snapshot.b04d7502d";


     //   String filePath="/home/xiaoniudu/share-doc2win7/zk/321-131/snapshot.b12a48f0e";

        //String filePath="/home/xiaoniudu/share-doc2win7/zk/321-131/snapshot.b12a60cdd";



      //  String filePath="/home/xiaoniudu/share-doc2win7/20150603/snapshot.1c1e18c165";


      //  String filePath="/home/xiaoniudu/share-doc2win7/20150603/snapshot.1c1e19dbc6";

        String filePath="/home/xiaoniudu/share-doc2win7/20150603/snapshot.1c1e1b0bac";


        //String filePath="/tmp/zookeeper1/version-2/snapshot.900000003";
        new SnapshotFormatter().run(filePath);
       // new SnapshotFormatter().run(args[0]);
    }
    
    public void run(String snapshotFileName) throws IOException {
        InputStream is = new CheckedInputStream(
                new BufferedInputStream(new FileInputStream(snapshotFileName)),
                new Adler32());
        InputArchive ia = BinaryInputArchive.getArchive(is);
        
        FileSnap fileSnap = new FileSnap(null);

        DataTree dataTree = new DataTree();
        Map<Long, Integer> sessions = new HashMap<Long, Integer>();
        
        fileSnap.deserialize(dataTree, sessions, ia);

        printDetails(dataTree, sessions);
    }

    private void printDetails(DataTree dataTree, Map<Long, Integer> sessions) {
        printZnodeDetails(dataTree);
     //   printSessionDetails(dataTree, sessions);
    }

    private void printZnodeDetails(DataTree dataTree) {
        System.out.println(String.format("ZNode Details (count=%d):",
                dataTree.getNodeCount()));
        
        printZnode(dataTree, "/");
        System.out.println("----");
    }

    private void printZnode(DataTree dataTree, String name) {
        System.out.println("----");
        DataNode n = dataTree.getNode(name);
        Set<String> children;
        synchronized(n) { // keep findbugs happy
            if(name.startsWith("/meta")){

            }else {
                System.out.println(name);
                printStat(n.stat);
                if (n.data != null) {
                    System.out.println("  dataLength = " + n.data.length);
                } else {
                    System.out.println("  no data");
                }
            }
            children = n.getChildren();
        }
        if (children != null) {
            for (String child : children) {
                printZnode(dataTree, name + (name.equals("/") ? "" : "/") + child);
            }
        }
    }

    private void printSessionDetails(DataTree dataTree, Map<Long, Integer> sessions) {
        System.out.println("Session Details (sid, timeout, ephemeralCount):");
        for (Map.Entry<Long, Integer> e : sessions.entrySet()) {
            long sid = e.getKey();
            System.out.println(String.format("%#016x, %d, %d",
                    sid, e.getValue(), dataTree.getEphemerals(sid).size()));
        }
    }

    private void printStat(StatPersisted stat) {
        printHex("cZxid", stat.getCzxid());
        System.out.println("  ctime = " + new Date(stat.getCtime()).toString());
        printHex("mZxid", stat.getMzxid());
        System.out.println("  mtime = " + new Date(stat.getMtime()).toString());
        printHex("pZxid", stat.getPzxid());
        System.out.println("  cversion = " + stat.getCversion());
        System.out.println("  dataVersion = " + stat.getVersion());
        System.out.println("  aclVersion = " + stat.getAversion());
        printHex("ephemeralOwner", stat.getEphemeralOwner());
    }

    private void printHex(String prefix, long value) {
        System.out.println(String.format("  %s = %#016x", prefix, value));
       // LOG.info(String.format("  %s = %#016x", prefix, value));
    }
}
