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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */
        
        public final static int CURRENTVERSION = 0x1; 
        int version;
                
        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        /*
         * epoch of the proposed leader
         */
        long peerEpoch;

        @Override
        public String toString() {
           return "electionEpoch:"+electionEpoch
                    +",leader:"+leader
                    +",zxid:"+Long.toHexString(zxid)
                    +",peerEpoch:"+Long.toHexString(peerEpoch)
                    +",sid:"+sid
                    +",state:"+state
                    +",msg version:"+
            Long.toHexString(version) ;
        }
    }

    static ByteBuffer buildMsg(int state,
            long leader,
            long zxid,
            long electionEpoch,
            long epoch) {
        byte requestBytes[] = new byte[40];//5*8个字节
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);

        return requestBuffer;
    }

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
                long leader,
                long zxid,
                long electionEpoch,
                ServerState state,
                long sid,
                long peerEpoch) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch  当前这一轮投票logicclock，lookForLeader()加1;
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Leader epoch ,xiaoniudu接收投票后，设置的值
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver implements Runnable {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                String pre="[选举-接收到投票]-";
                int i=0;
                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try{
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if(response == null) continue;
                        i++;

                        /*
                         * If it is from an observer, respond right away.
                         * Note that the following predicate assumes that
                         * if a server is not a follower, then it must be
                         * an observer. If we ever have any other type of
                         * learner in the future, we'll have to change the
                         * way we check for observers.
                         *
                         * 如果是Observer，则返回当前选举结果
                         */
                        if(!self.getVotingView().containsKey(response.sid)){
                            Vote current = self.getCurrentVote();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock,
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch());
                            LOG.info(pre+i+",发送投票的节点不在当前投票节点中，发送我当前的投票给他");
                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(pre+i+",Receive new notification message. My id = "
                                        + self.getId());
                            }

                            /*
                             * We check for 28 bytes for backward compatibility
                             */
                            if (response.buffer.capacity() < 28) {
                                LOG.error("Got a short response: "
                                        + response.buffer.capacity());
                                continue;
                            }
                            boolean backCompatibility = (response.buffer.capacity() == 28);
                            response.buffer.clear();

                            // Instantiate Notification and set its attributes
                            Notification n = new Notification();

                            // State of peer that sent this message 对方节点状态
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (response.buffer.getInt()) {
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }
                            //初始化Notification对象
                            n.leader = response.buffer.getLong();
                            n.zxid = response.buffer.getLong();
                            n.electionEpoch = response.buffer.getLong();
                            n.state = ackstate;
                            n.sid = response.sid;
                            if(!backCompatibility){
                                n.peerEpoch = response.buffer.getLong();
                            } else {
                                if(LOG.isInfoEnabled()){
                                    LOG.info("Backward compatibility mode, server id=" + n.sid);
                                }
                                n.peerEpoch = ZxidUtils.getEpochFromZxid(n.zxid);
                            }

                            /*
                             * Version added in 3.4.6
                             */

                            n.version = (response.buffer.remaining() >= 4) ?
                                         response.buffer.getInt() : 0x0;

                            /*
                             * Print notification info
                             */
                            if(LOG.isInfoEnabled()){
                                //printNotification(n);
                               LOG.info(pre+i+","+notification(n));
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             * 如果自己也在LOOKING，则放入业务接收队列，选举主线程会消费该消息
                             */
                            if(self.getPeerState() == QuorumPeer.ServerState.LOOKING){
                                recvqueue.offer(n);
                                LOG.info(pre+i+",我也在LOOKING中，将收到的投票添加到接收队列");
                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock)){//对方server也在选举状态，并且是上一轮选举，则把当前结果给他
                                    Vote v = getVote();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock,
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                    LOG.info(pre+i+",我也在LOOKING中，对方也在投票中，但对方属于上一轮，发送我当前的投票给他");
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                //如果自己不在选举状态，而对方server在LOOKING中，则向其发送当前的选举结果，当有server加入一个essemble时有用
                                if(ackstate == QuorumPeer.ServerState.LOOKING){
                                    if(LOG.isDebugEnabled()){
                                        LOG.debug(pre+i+",我不在LOOKING状态，对方在LOOKING中，发送我当前的投票给他，Sending new notification. 我是：My id =  " +
                                                self.getId() + " 对方是：recipient=" +
                                                response.sid + "，当前的： zxid=0x" +
                                                Long.toHexString(current.getZxid()) +
                                                " leader=" + current.getId());
                                    }

                                    ToSend notmsg;
                                    if(n.version > 0x0) {
                                        notmsg = new ToSend(
                                                ToSend.mType.notification,
                                                current.getId(),
                                                current.getZxid(),
                                                current.getElectionEpoch(),
                                                self.getPeerState(),
                                                response.sid,
                                                current.getPeerEpoch());

                                    } else {
                                        Vote bcVote = self.getBCVote();
                                        notmsg = new ToSend(
                                                ToSend.mType.notification,
                                                bcVote.getId(),
                                                bcVote.getZxid(),
                                                bcVote.getElectionEpoch(),
                                                self.getPeerState(),
                                                response.sid,
                                                bcVote.getPeerEpoch());
                                    }
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }


        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender implements Runnable {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager){
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if(m == null) continue;

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(),
                                                        m.leader,
                                                        m.zxid,
                                                        m.electionEpoch,
                                                        m.peerEpoch);
                manager.toSend(m.sid, requestBuffer);
            }
        }

        /**
         * Test if both send and receive queues are empty.
         */
        public boolean queueEmpty() {
            return (sendqueue.isEmpty() || recvqueue.isEmpty());
        }


        WorkerSender ws;
        WorkerReceiver wr;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {
            //启动业务层发送线程，将消息发给IO负责类QuorumCnxManager
            this.ws = new WorkerSender(manager);

            Thread t = new Thread(this.ws,
                    "WorkerSender[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();
            //启动业务层接受线程，从IO负责类QuorumCnxManager接受消息
            this.wr = new WorkerReceiver(manager);

            t = new Thread(this.wr,
                    "WorkerReceiver[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt(){
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    volatile long logicalclock; /* Election instance */
    long proposedLeader;    //投票后,serverid
    long proposedZxid;      //
    long proposedEpoch;     //


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock(){
        return logicalclock;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager){
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();//业务发送队列
        recvqueue = new LinkedBlockingQueue<Notification>();//业务接收队列
        this.messenger = new Messenger(manager);
    }

    private void leaveInstance(Vote v) {
        if(LOG.isDebugEnabled()){
            LOG.debug("About to leave FLE instance: leader="
                + v.getId() + ", zxid=0x" +
                Long.toHexString(v.getZxid()) + ", my id=" + self.getId()
                + ", my state=" + self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager(){
        return manager;
    }

    volatile boolean stop;
    public void shutdown(){
        stop = true;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }


    /**
     * Send notifications to all peers upon a change in our vote
     * 循环发送
     */
    private void sendNotifications() {
        String ssid="";
        for (QuorumServer server : self.getVotingView().values()) {
            long sid = server.id;
            ssid+=","+sid;
            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock,
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch);

            sendqueue.offer(notmsg);//添加到业务的发送队列，该队列会被WorkerSender消费
        }

        if(LOG.isDebugEnabled()){
            LOG.debug("发送"+myProposed()+ "-接受者:"+ ssid);
        }
    }


    private void printNotification(Notification n){
        LOG.info(notification(n));
    }

    private String notification(Notification n){
        return "接收到的提议: " + n.toString()
                + " 我的状态：" + self.getPeerState();
    }

    private void printMyPropose(){
        LOG.info(myProposed());
    }
    private String myProposed(){
        return "我的提议:logicalclock："+logicalclock+", proposedLeader:" +proposedLeader+",proposedZxid:"+ ZxidUtils.zxidToString(proposedZxid)+",proposedEpoch:"+proposedEpoch;
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param newId    Server identifier
     * @param newZxid  Last zxid observed by the issuer of this vote
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("比较投票结果：newid: " + newId
                +",newZxid:"+newZxid+"-"+Long.toHexString(newZxid)
                +",newEpoch:"+newEpoch+"-"+Long.toHexString(newEpoch)
                +",curId:"+curId
                +",curZxid:"+curZxid+"-"+Long.toHexString(curZxid)
                +",curEpoch:"+curEpoch+"-"+Long.toHexString(curEpoch)
        );
        if(self.getQuorumVerifier().getWeight(newId) == 0){
            return false;
        }
        
        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         *
         *  1.先比较epoch,
         *  2.然后在比较zxid,事务
         *  3.然后在比较id(myid).
         */
        
        return ((newEpoch > curEpoch) || 
                ((newEpoch == curEpoch) &&
                ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if
     * have sufficient to declare the end of the election round.
     *
     *  @param votes    Set of votes
     * // @param         Identifier of the vote received last
     * // @param zxid     zxid of the the vote received last
     */
    protected boolean termPredicate(
            HashMap<Long, Vote> votes,
            Vote vote) {

        HashSet<Long> set = new HashSet<Long>();

        /*
         * First make the views consistent. Sometimes peers will have
         * different zxids for a server depending on timing.
         */
        for (Map.Entry<Long,Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())){
                set.add(entry.getKey());
            }
        }

        return self.getQuorumVerifier().containsQuorum(set);
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch){

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != self.getId()){
            if(votes.get(leader) == null) predicate = false;
            else if(votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if(logicalclock != electionEpoch) {
            predicate = false;
        } 

        return predicate;
    }
    
    /**
     * This predicate checks that a leader has been elected. It doesn't
     * make a lot of sense without context (check lookForLeader) and it
     * has been separated for testing purposes.
     * 
     * @param recv  map of received votes 
     * @param ooe   map containing out of election votes (LEADING or FOLLOWING)
     * @param n     Notification
     * @return          
     */
    protected boolean ooePredicate(HashMap<Long,Vote> recv,
                                    HashMap<Long,Vote> ooe,
                                    Notification n) {
        
        return (termPredicate(recv, new Vote(n.version,
                                             n.leader,
                                             n.zxid, 
                                             n.electionEpoch, 
                                             n.peerEpoch, 
                                             n.state))
                && checkLeader(ooe, n.leader, n.electionEpoch));
        
    }

    synchronized void updateProposal(long leader, long zxid, long epoch){
        if(LOG.isDebugEnabled()){
            LOG.debug("更新我的提议:(leader)" + leader
                    + ",(zxid) 0x" + Long.toHexString(zxid)
                    + ",(epoch)："+epoch
                    + "上一次我的："
                    + "(leader)：" + proposedLeader
                    + ",(zxid)： 0x" + Long.toHexString(proposedZxid)
                    + ",(epoch)："+proposedEpoch);
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized Vote getVote(){
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT){
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        }
        else{
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
        	try {
        		return self.getCurrentEpoch();
        	} catch(IOException e) {
        		RuntimeException re = new RuntimeException(e.getMessage());
        		re.setStackTrace(e.getStackTrace());
        		throw re;
        	}
        else return Long.MIN_VALUE;
    }
    
    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
           self.start_fle = System.currentTimeMillis();
        }
        try {
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();//收到的投票

            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = finalizeWait;

            synchronized(this){
                logicalclock++;//新一轮投票
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());//初始化选票，投给自己
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            sendNotifications();//发送投票

            /*
             * Loop in which we exchange notifications until we find a leader 主循环，直到选出Leader
             */
            int i=0;
            while ((self.getPeerState() == ServerState.LOOKING) &&
                    (!stop)){
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                Notification n = recvqueue.poll(notTimeout,
                        TimeUnit.MILLISECONDS);//拿投递的消息，自己的也在这里
                i++;
                String pre="[选举-处理接收到的提议]-"+i;
                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                if(n == null){//没有收到消息
                    LOG.info(pre+",没有收到提议，将重新发送或者对没有连接的建立连接");
                    if(manager.haveDelivered()){
                        sendNotifications();//自己的消息投递完了，继续重复投递
                    } else {
                        manager.connectAll();//自己的消息没有投递完，可能其他server还没有启动，尝试连接
                    }

                    /*
                     * Exponential backoff 延长超时时间
                     */
                    int tmpTimeOut = notTimeout*2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval?
                            tmpTimeOut : maxNotificationInterval);
                    LOG.info(pre+",下一次从队列读数据阻塞时间，Notification time out: " + notTimeout);
                }
                else if(self.getVotingView().containsKey(n.sid)) {//收到投递消息
                    LOG.info(pre+",收到提议，提议发送者处于"+n.state.name()+",节点："+n.sid);
                    /*
                     * Only proceed if the vote comes from a replica in the
                     * voting view.
                     */
                    switch (n.state) {
                    case LOOKING:
                        //检查下收到的这张选票是否可以胜出，依次比较选举轮数epoch，事务zxid，服务器编号server id，
                        //胜出了，就把自己的投票修改为对方的，然后广播消息
                        // If notification > current, replace and send messages out
                        if (n.electionEpoch > logicalclock) {
                            LOG.info(pre+",收到的提议是新一轮，更新逻辑时钟，清理已经收到待计算的提议");
                            logicalclock = n.electionEpoch;//###xiaoniudu 最新一次的投票，更新logicalclock
                            recvset.clear();
                            if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                    getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            } else {
                                updateProposal(getInitId(),
                                        getInitLastLoggedZxid(),
                                        getPeerEpoch());
                            }
                            LOG.info(pre+",比较后更新支持的提议，将发送支持的提议，"+myProposed());
                            sendNotifications();
                        } else if (n.electionEpoch < logicalclock) {//###xiaoniudu 老的一次投票（上轮投票），不予理睬
                            if(LOG.isDebugEnabled()){
                                LOG.debug(pre+",收到的是上一轮，丢弃。Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                        + Long.toHexString(n.electionEpoch)
                                        + ", logicalclock=0x" + Long.toHexString(logicalclock));
                            }
                            break;
                        } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                proposedLeader, proposedZxid, proposedEpoch)) {//###xiaoniudu 同一轮投票，对方的投票优于当前的投票，更新当前的投票，发生投票结果通知，否则，对方投票低于当前投票，不于处理
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            LOG.info(pre+",收到的提议是同一轮，接受新的提议，发送新提议，"+myProposed());
                            sendNotifications();
                        }

                        if(LOG.isDebugEnabled()){
                            LOG.debug(pre+",将收到的提议加入表决队列。Adding vote: from=" + n.sid +
                                    ", proposed leader=" + n.leader +
                                    ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                    ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                        }

                        //添加到本机投票集合，用来做选举终结判断
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));//###添加投票

                        LOG.debug(pre+",计算投票开始");
                        //选举是否结束，默认算法是超过半数server同意
                        if (termPredicate(recvset,
                                new Vote(proposedLeader, proposedZxid,
                                        logicalclock, proposedEpoch))) {//###xiaoniudu 计算投票结果得到多数同意

                            LOG.debug(pre+",表决通过,"+myProposed());
                            // Verify if there is any change in the proposed leader
                            // ###xiaoniudu 等待一段时间看是否有比当前更优的提议
                            while((n = recvqueue.poll(finalizeWait,
                                    TimeUnit.MILLISECONDS)) != null){
                                if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                        proposedLeader, proposedZxid, proposedEpoch)){
                                    recvqueue.put(n);//###xiaoniudu 有比当前更优的提议，放回去等待下一次大循环再执行
                                    LOG.debug(pre+",表决通过后还收到更优提议，重来-----");
                                    break;
                                }
                            }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             * ###xiaoniudu 当还偶提议但不优于当前提议时，跳回到上面的while,但n为null,#todo 一直在循环840行,是否会有bug
                             */
                            if (n == null) {
                                self.setPeerState((proposedLeader == self.getId()) ?
                                        ServerState.LEADING: learningState());//修改状态，LEADING or FOLLOWING

                                Vote endVote = new Vote(proposedLeader,
                                                        proposedZxid,
                                                        logicalclock,
                                                        proposedEpoch);

                                LOG.debug(pre+",表决通过，我是:"+self.getPeerState().name()+",结束选举");
                                leaveInstance(endVote);
                                return endVote;//返回最终的选票结果
                            }
                        }
                        break;

                    //如果收到的选票状态不是LOOKING，比如这台机器刚加入一个已经服务的zk集群时发送的选举
                    case OBSERVING:
                        LOG.debug(pre+",Notification from observer: " + n.sid);
                        break;
                    case FOLLOWING:
                    case LEADING:
                        /*
                         * Consider all notifications from the same epoch
                         * together.
                         */
                        if(n.electionEpoch == logicalclock){//同一轮选举，加入投票
                            LOG.debug(pre+",是同一轮选举，将收到的提议加入表决队列。");
                            recvset.put(n.sid, new Vote(n.leader,
                                                          n.zxid,
                                                          n.electionEpoch,
                                                          n.peerEpoch));
                            LOG.debug(pre+",计算投票开始");
                            //投票是否结束，如果结束，再确认LEADER是否有效
                            if(ooePredicate(recvset, outofelection, n)) {
                                self.setPeerState((n.leader == self.getId()) ?
                                        ServerState.LEADING: learningState());

                                LOG.debug(pre+",表决通过，我是:"+self.getPeerState().name()+",结束选举");
                                Vote endVote = new Vote(n.leader,
                                        n.zxid, 
                                        n.electionEpoch, 
                                        n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }else {
                            LOG.debug(pre+",不是同一轮选举。");
                        }

                        /*
                         * Before joining an established ensemble, verify
                         * a majority is following the same leader.
                         */
                        outofelection.put(n.sid, new Vote(n.version,
                                                            n.leader,
                                                            n.zxid,
                                                            n.electionEpoch,
                                                            n.peerEpoch,
                                                            n.state));
           
                        if(ooePredicate(outofelection, outofelection, n)) {
                            synchronized(this){
                                logicalclock = n.electionEpoch;
                                self.setPeerState((n.leader == self.getId()) ?
                                        ServerState.LEADING: learningState());
                            }
                            Vote endVote = new Vote(n.leader,
                                                    n.zxid,
                                                    n.electionEpoch,
                                                    n.peerEpoch);
                            LOG.debug(pre+",表决通过，我是:"+self.getPeerState().name()+",结束选举");
                            leaveInstance(endVote);
                            return endVote;
                        }
                        break;
                    default:
                        LOG.warn("Notification state unrecognized: {} (n.state), {} (n.sid)",
                                n.state, n.sid);
                        break;
                    }
                } else {
                    LOG.warn(pre+",Ignoring notification from non-cluster member（忽略没有在当前集群中的投票消息） " + n.sid);
                }
            }
            return null;
        } finally {
            try {
                if(self.jmxLeaderElectionBean != null){
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }
    }
}
