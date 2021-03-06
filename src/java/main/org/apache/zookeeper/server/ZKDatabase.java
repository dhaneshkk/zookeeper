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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.AccessControlList;
import org.apache.zookeeper.server.Request.Meta;
import org.apache.zookeeper.server.Transaction.ProcessTxnResult;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.PlayBackListener;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains the in memory database of zookeeper
 * server states that includes the sessions, datatree and the
 * committed logs. It is booted up  after reading the logs
 * and snapshots from the disk.
 */
public class ZKDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(ZKDatabase.class);

    /**
     * make sure on a clear you take care of
     * all these members.
     */
    protected DataTree dataTree;
    public final ConcurrentHashMap<Long, Integer> sessionsWithTimeouts = new ConcurrentHashMap<Long, Integer>();
    protected long minCommittedLog, maxCommittedLog;
    public static final int commitLogCount = 500;
    protected static int commitLogBuffer = 700;
    protected LinkedList<Proposal> committedLog = new LinkedList<Proposal>();
    protected ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
    volatile private boolean initialized = false;

    public final WatchManager dataWatches = new WatchManager();

    public final WatchManager childWatches = new WatchManager();

    /**
     * the filetxnsnaplog that this zk database
     * maps to. There is a one to one relationship
     * between a filetxnsnaplog and zkdatabase.
     * @param snapLog the FileTxnSnapLog mapping this zkdatabase
     */
    public ZKDatabase() {
        dataTree = new DataTree();
    }

    /**
     * checks to see if the zk database has been
     * initialized or not.
     * @return true if zk database is initialized and false if not
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * clear the zkdatabase.
     * Note to developers - be careful to see that
     * the clear method does clear out all the
     * data structures in zkdatabase.
     */
    public void clear() {
        minCommittedLog = 0;
        maxCommittedLog = 0;
        /* to be safe we just create a new
         * datatree.
         */
        dataTree = new DataTree();
        sessionsWithTimeouts.clear();
        WriteLock lock = logLock.writeLock();
        try {
            lock.lock();
            committedLog.clear();
        } finally {
            lock.unlock();
        }
        initialized = false;
    }

    /**
     * the datatree for this zkdatabase
     * @return the datatree for this zkdatabase
     */
    public DataTree getDataTree() {
        return this.dataTree;
    }

    /**
     * the committed log for this zk database
     * @return the committed log for this zkdatabase
     */
    public long getmaxCommittedLog() {
        return maxCommittedLog;
    }


    /**
     * the minimum committed transaction log
     * available in memory
     * @return the minimum committed transaction
     * log available in memory
     */
    public long getminCommittedLog() {
        return minCommittedLog;
    }
    /**
     * Get the lock that controls the committedLog. If you want to get the pointer to the committedLog, you need
     * to use this lock to acquire a read lock before calling getCommittedLog()
     * @return the lock that controls the committed log
     */
    public ReentrantReadWriteLock getLogLock() {
        return logLock;
    }


    public synchronized List<Proposal> getCommittedLog() {
        ReadLock rl = logLock.readLock();
        // only make a copy if this thread isn't already holding a lock
        if(logLock.getReadHoldCount() <=0) {
            try {
                rl.lock();
                return new LinkedList<Proposal>(this.committedLog);
            } finally {
                rl.unlock();
            }
        }
        return this.committedLog;
    }

    /**
     * get the last processed zxid from a datatree
     * @return the last processed zxid of a datatree
     */
    public long getDataTreeLastProcessedZxid() {
        return dataTree.lastProcessedZxid;
    }

    /**
     * return the sessions in the datatree
     * @return the data tree sessions
     */
    public Collection<Long> getSessions() {
        return dataTree.getSessions();
    }

    /**
     * load the database from the disk onto memory and also add
     * the transactions to the committedlog in memory.
     * @return the last valid zxid on disk
     * @throws IOException
     */
    public long loadDataBase(FileTxnSnapLog snapLog) throws IOException {
        PlayBackListener listener=new PlayBackListener(){
            public void onTxnLoaded(TxnHeader hdr,Record txn){
                Meta meta = new Meta(0, hdr.getCxid(), hdr.getZxid(), OpCode.fromInt(hdr.getType()));
                Request r = new Request(meta, null, hdr, txn);
                addCommittedProposal(r);
            }
        };

        dataTree = snapLog.restore(sessionsWithTimeouts,listener);
        long zxid = dataTree.lastProcessedZxid;
        initialized = true;
        return zxid;
    }

    /**
     * maintains a list of last <i>committedLog</i>
     *  or so committed requests. This is used for
     * fast follower synchronization.
     * @param request committed request
     */
    public void addCommittedProposal(Request request) {
        WriteLock wl = logLock.writeLock();
        try {
            wl.lock();
            if (committedLog.size() > commitLogCount) {
                committedLog.removeFirst();
                minCommittedLog = committedLog.getFirst().packet.getZxid();
            }
            if (committedLog.isEmpty()) {
                minCommittedLog = request.getMeta().getZxid();
                maxCommittedLog = request.getMeta().getZxid();
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            try {
                request.getHdr().serialize(boa, "hdr");
                if (request.getTxn() != null) {
                    request.getTxn().serialize(boa, "txn");
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("This really should be impossible", e);
            }
            QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.getMeta().getZxid(), baos.toByteArray(), null);
            Proposal p = new Proposal();
            p.packet = pp;
            p.request = request;
            committedLog.add(p);
            maxCommittedLog = p.packet.getZxid();
        } finally {
            wl.unlock();
        }
    }


    /**
     * remove a cnxn from the datatree
     * @param cnxn the cnxn to remove from the datatree
     */
    public void removeCnxn(ServerCnxn watcher) {
        dataWatches.removeWatcher(watcher);
        childWatches.removeWatcher(watcher);
    }

    /**
     * write a text dump of all the ephemerals in the datatree
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        dataTree.dumpEphemerals(pwriter);
    }

    /**
     * the node count of the datatree
     * @return the node count of datatree
     */
    public int getNodeCount() {
        return dataTree.getNodeCount();
    }

    /**
     * the paths for  ephemeral session id
     * @param sessionId the session id for which paths match to
     * @return the paths for a session id
     */
    public Set<String> getEphemerals(long sessionId) {
        return dataTree.getEphemerals(sessionId);
    }

    /**
     * the last processed zxid in the datatree
     * @param zxid the last processed zxid in the datatree
     */
    public void setlastProcessedZxid(long zxid) {
        dataTree.lastProcessedZxid = zxid;
    }

    /**
     * get the datanode for this path
     * @param path the path to lookup
     * @return the datanode for getting the path
     */
    public DataNode getNode(String path) {
      return dataTree.getNode(path);
    }

    /**
     * convert from long to the acl entry
     * @param aclL the long for which to get the acl
     * @return the acl corresponding to this long entry
     */
    public AccessControlList convertLong(Long aclL) {
        return dataTree.convertLong(aclL);
    }

    /**
     * check if the path is special or not
     * @param path the input path
     * @return true if path is special and false if not
     */
    public boolean isSpecialPath(String path) {
        return dataTree.isSpecialPath(path);
    }

    /**
     * get the acl size of the datatree
     * @return the acl size of the datatree
     */
    public int getAclSize() {
        return dataTree.getAclSize();
    }

    /**
     * truncate the zkdatabase to this zxid
     * @param zxid the zxid to truncate zk database to
     * @return true if the truncate is succesful and false if not
     * @throws IOException
     */
    public boolean truncateLog(FileTxnSnapLog snapLog, long zxid) throws IOException {
        clear();
        boolean truncated = snapLog.truncateLog(zxid);
        loadDataBase(snapLog);
        return truncated;
    }

    /**
     * deserialize a snapshot from an input archive
     * @param ia the input archive you want to deserialize from
     * @throws IOException
     */
    public void deserializeSnapshot(InputArchive ia) throws IOException {
        clear();
        dataTree = SerializeUtils.deserializeSnapshot(ia, sessionsWithTimeouts);
        initialized = true;
    }

    /**
     * serialize the snapshot
     * @param oa the output archive to which the snapshot needs to be serialized
     * @throws IOException
     * @throws InterruptedException
     */
    public void serializeSnapshot(OutputArchive oa) throws IOException,
    InterruptedException {
        SerializeUtils.serializeSnapshot(getDataTree(), oa, sessionsWithTimeouts);
    }

    public void processTxn(TxnHeader hdr, Record txn) {
        try {
            Transaction transaction = Transaction.fromTxn(hdr, txn);
            ProcessTxnResult rc = transaction.process(dataTree);
            rc.trigger.triggerWatches(this);
        } catch (KeeperException e) {
            LOG.warn("Failed: ", e);
        }
    }

    public int getWatchCount() {
        return dataWatches.size() + childWatches.size();
    }

    /**
     * Summary of the watches on the datatree.
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatchesSummary(PrintWriter pwriter) {
        pwriter.print(dataWatches.toString());
    }

    /**
     * Write a text dump of all the watches on the datatree.
     * Warning, this is expensive, use sparingly!
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        dataWatches.dumpWatches(pwriter, byPath);
    }
}
