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

package org.apache.zookeeper.server.quorum;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This class has the control logic for the Follower.
 */
public class Follower extends Learner{

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;
    
    Follower(QuorumPeer self,FollowerZooKeeperServer zk) {
        this.self = self;
        this.zk=zk;
        this.fzk = zk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:")
            .append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * 1.查找leader ip
     * 2.连接leader
     * 3.传递learner的zxid给leader
     * 4.leader传递zxid给learner
     * 5.与leader同步数据
     * 6.进入正常的leader与learner交互流程
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    void followLeader() throws InterruptedException {
        self.end_fle = System.currentTimeMillis();
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - " +
              (self.end_fle - self.start_fle));
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);
        try {
            InetSocketAddress addr = findLeader(); //查找leader ip
            try {
                connectToLeader(addr);//连接leader
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);

                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch " + ZxidUtils.zxidToString(newEpochZxid)
                            + " is less than our accepted epoch " + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                syncWithLeader(newEpochZxid);
                LOG.info("选举后follower同步leader结束，同步结束，开始正常工作---------------");
                QuorumPacket qp = new QuorumPacket();
                while (self.isRunning()) {
                    readPacket(qp);
                    processPacket(qp);//###xiaoniud appept leader ping and others
                }
            } catch (Exception e) {
                LOG.warn("Exception when following the leader", e);
                try {
                    sock.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
    
                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX((Learner)this);
        }
    }

    /**
     *
     *
     *
     * Follower的消息循环处理如下几种来自Leader的消息

     1.  PING 心跳消息，返回PING消息给Leader

     2. PROPOSAL消息：放入pendingTxns队列，然后转发给SyncRequestProcessor线程

     3. COMMIT消息：取出pendingTxns队列中的第一个消息，与这个commit消息比较，如果两者zxid相同，提交给commitProcessor线程处理；
     如果zxid不同，说明之间有消息丢失，本节点的数据已经不一致了。直接退出server！等下次重启时，再通过和Leader的交互完成数据的同步。

     4. UPTODATE消息：Follower开始接入时，在Leader发送完对Follower的同步指令之后，发送这个消息，表示follower可以提供服务了。
     follower处理该消息时，表名同步已经完成，将当前日志写入snap文件持久化。

     5. REVALIDATE消息：根据Leader的REVALIDATE结果，关闭待revalidate的session还是允许其接受消息

     6. SYNC消息：返回SYNC结果到客户端。这个消息最初由客户端发起，用来强制得到最新的更新。对应于Paxos协议中的慢速读。
     *
     *
     *
     * Examine the packet received in qp and dispatch based on its contents.
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws IOException{
        if(qp.getType()!=Leader.PING) {
            LOG.info("接收到leader的数据包(过滤了PING)，type：{},zxid:{},zxid:{}", new Object[]{Leader.getPacketType(qp.getType()),qp.getZxid(),ZxidUtils.zxidToString(qp.getZxid())});
        }
        switch (qp.getType()) {
        case Leader.PING:            
            ping(qp);//发送活动的session给leader
            break;
        case Leader.PROPOSAL:            
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);
            if (hdr.getZxid() != lastQueued + 1) {
                LOG.warn("Got zxid 0x"
                        + Long.toHexString(hdr.getZxid())
                        + " expected 0x"
                        + Long.toHexString(lastQueued + 1));
            }
            lastQueued = hdr.getZxid();
            fzk.logRequest(hdr, txn);//处理与leader投票请求,对应syncRequestProcessor
            break;
        case Leader.COMMIT:
            fzk.commit(qp.getZxid());//对应commitProcessor
            break;
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Follower started");
            break;
        case Leader.REVALIDATE:
            revalidate(qp);
            break;
        case Leader.SYNC:
            fzk.sync();//对应commitProcessor
            break;
        }
    }

    /**
     * The zxid of the last operation seen
     * @return zxid
     */
    public long getZxid() {
        try {
            synchronized (fzk) {
                return fzk.getZxid();
            }
        } catch (NullPointerException e) {
            LOG.warn("error getting zxid", e);
        }
        return -1;
    }
    
    /**
     * The zxid of the last operation queued
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    @Override
    public void shutdown() {    
        LOG.info("shutdown called", new Exception("shutdown Follower"));
        super.shutdown();
    }
}
