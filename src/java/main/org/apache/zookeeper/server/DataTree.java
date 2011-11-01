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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Index;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.common.AccessControlList;
import org.apache.zookeeper.common.Path;
import org.apache.zookeeper.common.PathTrie;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains the tree data structure. It doesn't have any networking
 * or client connection code in it so that it can be tested in a stand alone
 * way.
 * <p>
 * The tree maintains two parallel data structures: a hashtable that maps from
 * full paths to DataNodes and a tree of DataNodes. All accesses to a path is
 * through the hashtable. The tree is traversed only when serializing to disk.
 */
public class DataTree {
    private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);

    public volatile long lastProcessedZxid = 0;

    /**
     * This hashtable provides a fast lookup to the datanodes. The tree is the
     * source of truth and is where all the locking occurs
     */
    final ConcurrentHashMap<String, DataNode> nodes =
        new ConcurrentHashMap<String, DataNode>();

    public final WatchManager dataWatches = new WatchManager();

    public final WatchManager childWatches = new WatchManager();

    /** the root of zookeeper tree */
    private static final String rootZookeeper = "/";

    /** the zookeeper nodes that acts as the management and status node **/
    private static final String procZookeeper = Quotas.procZookeeper;

    /** this will be the string thats stored as a child of root */
    private static final String procChildZookeeper = procZookeeper.substring(1);

    /**
     * the zookeeper quota node that acts as the quota management node for
     * zookeeper
     */
    private static final String quotaZookeeper = Quotas.quotaZookeeper;

    /** this will be the string thats stored as a child of /zookeeper */
    private static final String quotaChildZookeeper = quotaZookeeper
            .substring(procZookeeper.length() + 1);

    /**
     * the path trie that keeps track fo the quota nodes in this datatree
     */
    final PathTrie pTrie = new PathTrie();

    /**
     * This hashtable lists the paths of the ephemeral nodes of a session.
     */
    final Map<Long, HashSet<String>> ephemerals =
        new ConcurrentHashMap<Long, HashSet<String>>();

    /**
     * this is map from longs to acl's. It saves acl's being stored for each
     * datanode.
     */
    private final Map<Long, AccessControlList> longKeyMap =
        new HashMap<Long, AccessControlList>();

    /**
     * this a map from acls to long.
     */
    private final Map<AccessControlList, Long> aclKeyMap =
        new HashMap<AccessControlList, Long>();

    /**
     * these are the number of acls that we have in the datatree
     */
    private long aclIndex = 0;

    @SuppressWarnings("unchecked")
    public Set<String> getEphemerals(long sessionId) {
        HashSet<String> retv = ephemerals.get(sessionId);
        if (retv == null) {
            return new HashSet<String>();
        }
        HashSet<String> cloned = null;
        synchronized (retv) {
            cloned = (HashSet<String>) retv.clone();
        }
        return cloned;
    }

    int getAclSize() {
        return longKeyMap.size();
    }

    private long incrementIndex() {
        return ++aclIndex;
    }

    /**
     * converts the list of acls to a list of longs.
     *
     * @param acls
     * @return a list of longs that map to the acls
     */
    public synchronized Long convertAcls(AccessControlList acl) {
        if (acl == null || acl == AccessControlList.EMPTY)
            return -1L;
        // get the value from the map
        Long ret = aclKeyMap.get(acl);
        // could not find the map
        if (ret != null)
            return ret;
        long val = incrementIndex();
        longKeyMap.put(val, acl);
        aclKeyMap.put(acl, val);
        return val;
    }

    /**
     * converts a list of longs to a list of acls.
     *
     * @param longVal
     *            the list of longs
     * @return a list of ACLs that map to longs
     */
    public synchronized AccessControlList convertLong(Long longVal) {
        if (longVal == null)
            return null;
        if (longVal == -1L)
            return AccessControlList.OPEN_ACL_UNSAFE;
        AccessControlList acl = longKeyMap.get(longVal);
        if (acl == null) {
            LOG.error("ERROR: ACL not available for long " + longVal);
            throw new RuntimeException("Failed to fetch acls for " + longVal);
        }
        return acl;
    }

    public Collection<Long> getSessions() {
        return ephemerals.keySet();
    }

    public DataNode getNode(String path) {
        return nodes.get(path);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public int getWatchCount() {
        return dataWatches.size() + childWatches.size();
    }

    int getEphemeralsCount() {
        int result = 0;
        for (HashSet<String> set : ephemerals.values()) {
            result += set.size();
        }
        return result;
    }

    /**
     * Get the size of the nodes based on path and data length.
     *
     * @return size of the data
     */
    public long approximateDataSize() {
        long result = 0;
        for (Map.Entry<String, DataNode> entry : nodes.entrySet()) {
            DataNode value = entry.getValue();
            synchronized (value) {
                result += entry.getKey().length();
                result += value.getApproximateDataSize();
            }
        }
        return result;
    }

    /**
     * This is a pointer to the root of the DataTree. It is the source of truth,
     * but we usually use the nodes hashmap to find nodes in the tree.
     */
    private DataNode root = new DataNode(new byte[0], -1L, new StatPersisted());

    /**
     * create a /zookeeper filesystem that is the proc filesystem of zookeeper
     */
    private final DataNode procDataNode = new DataNode(new byte[0], -1L, new StatPersisted());

    /**
     * create a /zookeeper/quota node for maintaining quota properties for
     * zookeeper
     */
    private final DataNode quotaDataNode = new DataNode(new byte[0], -1L, new StatPersisted());

    public DataTree() {
        /* Rather than fight it, let root have an alias */
        nodes.put("", root);
        nodes.put(rootZookeeper, root);

        /** add the proc node and quota node */
        root.addChild(procChildZookeeper);
        nodes.put(procZookeeper, procDataNode);

        procDataNode.addChild(quotaChildZookeeper);
        nodes.put(quotaZookeeper, quotaDataNode);
    }

    /**
     * is the path one of the special paths owned by zookeeper.
     *
     * @param path
     *            the path to be checked
     * @return true if a special path. false if not.
     */
    boolean isSpecialPath(String path) {
        if (rootZookeeper.equals(path) || procZookeeper.equals(path)
                || quotaZookeeper.equals(path)) {
            return true;
        }
        return false;
    }

    static public void copyStatPersisted(StatPersisted from, StatPersisted to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
    }

    /**
     * update the count of this stat datanode
     *
     * @param lastPrefix
     *            the path of the node that is quotaed.
     * @param diff
     *            the diff to be added to the count
     */
    public void updateCount(String lastPrefix, int diff) {
        String statNode = Quotas.statPath(lastPrefix);
        DataNode node = nodes.get(statNode);
        StatsTrack updatedStat = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for stat " + statNode);
            return;
        }
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data));
            updatedStat.setCount(updatedStat.getCount() + diff);
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the counts match the quota
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        StatsTrack thisStats = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for quota " + quotaNode);
            return;
        }
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getCount() > -1 && (thisStats.getCount() < updatedStat.getCount())) {
            LOG
            .warn("Quota exceeded: " + lastPrefix + " count="
                    + updatedStat.getCount() + " limit="
                    + thisStats.getCount());
        }
    }

    /**
     * update the count of bytes of this stat datanode
     *
     * @param lastPrefix
     *            the path of the node that is quotaed
     * @param diff
     *            the diff to added to number of bytes
     * @throws IOException
     *             if path is not found
     */
    public void updateBytes(String lastPrefix, long diff) {
        String statNode = Quotas.statPath(lastPrefix);
        DataNode node = nodes.get(statNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing stat node for bytes " + statNode);
            return;
        }
        StatsTrack updatedStat = null;
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data));
            updatedStat.setBytes(updatedStat.getBytes() + diff);
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the bytes match the quota
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing quota node for bytes " + quotaNode);
            return;
        }
        StatsTrack thisStats = null;
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getBytes() > -1 && (thisStats.getBytes() < updatedStat.getBytes())) {
            LOG
            .warn("Quota exceeded: " + lastPrefix + " bytes="
                    + updatedStat.getBytes() + " limit="
                    + thisStats.getBytes());
        }
    }

    public void createNode(final String path, byte data[], List<ACL> acl,
            long ephemeralOwner, int parentCVersion, long zxid, long time)
            throws KeeperException.NoNodeException,
            KeeperException.NodeExistsException {
        TxnHeader hdr = new TxnHeader();
        hdr.setClientId(ephemeralOwner);
        hdr.setZxid(zxid);
        hdr.setTime(time);

        CreateTxn txn = new CreateTxn(path, data, acl, true, parentCVersion);
        Transaction.Create createTransaction = new Transaction.Create(hdr, txn);
        createTransaction.process(this);
        createTransaction.triggerWatches(this);
    }

    /**
     * If there is a quota set, return the appropriate prefix for that quota
     * Else return null
     * @param path The ZK path to check for quota
     * @return Max quota prefix, or null if none
     */
    public String getMaxPrefixWithQuota(Path path) {
        // do nothing for the root.
        // we are not keeping a quota on the zookeeper
        // root node for now.
        String lastPrefix = pTrie.findMaxPrefix(path.toString());

        if (rootZookeeper.equals(lastPrefix) || "".equals(lastPrefix)) {
            return null;
        }
        else {
            return lastPrefix;
        }
    }

    /**
     * a encapsultaing class for return value
     */
    private static class Counts {
        long bytes;
        int count;
    }

    /**
     * this method gets the count of nodes and the bytes under a subtree
     *
     * @param path
     *            the path to be used
     * @param counts
     *            the int count
     */
    private void getCounts(String path, Counts counts) {
        DataNode node = getNode(path);
        if (node == null) {
            return;
        }
        String[] children = null;
        int len = 0;
        synchronized (node) {
            Set<String> childs = node.getChildren();
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]);
            }
            len = (node.data == null ? 0 : node.data.length);
        }
        // add itself
        counts.count += 1;
        counts.bytes += len;
        if (children == null || children.length == 0) {
            return;
        }
        for (String child : children) {
            getCounts(path + "/" + child, counts);
        }
    }

    /**
     * update the quota for the given path
     *
     * @param path
     *            the path to be used
     */
    void updateQuotaForPath(String path) {
        Counts c = new Counts();
        getCounts(path, c);
        StatsTrack strack = new StatsTrack();
        strack.setBytes(c.bytes);
        strack.setCount(c.count);
        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode;
        DataNode node = getNode(statPath);
        // it should exist
        if (node == null) {
            LOG.warn("Missing quota stat node " + statPath);
            return;
        }
        synchronized (node) {
            node.data = strack.toString().getBytes();
        }
    }

    /**
     * this method traverses the quota path and update the path trie and sets
     *
     * @param path
     */
    private void traverseNode(String path) {
        DataNode node = getNode(path);
        String children[] = null;
        synchronized (node) {
            Set<String> childs = node.getChildren();
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]);
            }
        }
        if (children == null || children.length == 0) {
            // this node does not have a child
            // is the leaf node
            // check if its the leaf node
            String endString = "/" + Quotas.limitNode;
            if (path.endsWith(endString)) {
                // ok this is the limit node
                // get the real node and update
                // the count and the bytes
                String realPath = path.substring(Quotas.quotaZookeeper
                        .length(), path.indexOf(endString));
                updateQuotaForPath(realPath);
                this.pTrie.addPath(realPath);
            }
            return;
        }
        for (String child : children) {
            traverseNode(path + "/" + child);
        }
    }

    /**
     * this method sets up the path trie and sets up stats for quota nodes
     */
    private void setupQuota() {
        String quotaPath = Quotas.quotaZookeeper;
        DataNode node = getNode(quotaPath);
        if (node == null) {
            return;
        }
        traverseNode(quotaPath);
    }

    /**
     * this method uses a stringbuilder to create a new path for children. This
     * is faster than string appends ( str1 + str2).
     *
     * @param oa
     *            OutputArchive to write to.
     * @param path
     *            a string builder.
     * @throws IOException
     * @throws InterruptedException
     */
    void serializeNode(OutputArchive oa, StringBuilder path) throws IOException {
        String pathString = path.toString();
        DataNode node = getNode(pathString);
        if (node == null) {
            return;
        }
        String children[] = null;
        synchronized (node) {
            oa.writeString(pathString, "path");
            SerializeUtils.serializeNode(oa, node);
            Set<String> childs = node.getChildren();
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]);
            }
        }
        path.append('/');
        int off = path.length();
        if (children != null) {
            for (String child : children) {
                // since this is single buffer being resused
                // we need
                // to truncate the previous bytes of string.
                path.delete(off, Integer.MAX_VALUE);
                path.append(child);
                serializeNode(oa, path);
            }
        }
    }

    private void deserializeList(Map<Long, AccessControlList> longKeyMap,
            InputArchive ia) throws IOException {
        int i = ia.readInt("map");
        while (i > 0) {
            Long val = ia.readLong("long");
            if (aclIndex < val) {
                aclIndex = val;
            }
            List<ACL> aclList = new ArrayList<ACL>();
            Index j = ia.startVector("acls");
            while (!j.done()) {
                ACL acl = new ACL();
                acl.deserialize(ia, "acl");
                aclList.add(acl);
                j.incr();
            }
            AccessControlList accessControlList = AccessControlList.fromJuteACL(aclList);
            longKeyMap.put(val, accessControlList);
            aclKeyMap.put(accessControlList, val);
            i--;
        }
    }

    private synchronized void serializeList(Map<Long, AccessControlList> longKeyMap,
            OutputArchive oa) throws IOException {
        oa.writeInt(longKeyMap.size(), "map");
        Set<Map.Entry<Long, AccessControlList>> set = longKeyMap.entrySet();
        for (Map.Entry<Long, AccessControlList> val : set) {
            oa.writeLong(val.getKey(), "long");
            List<ACL> aclList = val.getValue().toJuteACL();
            oa.startVector(aclList, "acls");
            for (ACL acl : aclList) {
                acl.serialize(oa, "acl");
            }
            oa.endVector(aclList, "acls");
        }
    }

    public void serialize(OutputArchive oa, String tag) throws IOException {
        serializeList(longKeyMap, oa);
        serializeNode(oa, new StringBuilder(""));
        // / marks end of stream
        // we need to check if clear had been called in between the snapshot.
        if (root != null) {
            oa.writeString("/", "path");
        }
    }

    public void deserialize(InputArchive ia, String tag) throws IOException {
        deserializeList(longKeyMap, ia);
        nodes.clear();
        String path = ia.readString("path");
        while (!"/".equals(path)) {
            DataNode node = SerializeUtils.deserializeNode(ia);
            nodes.put(path, node);
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1) {
                root = node;
            } else {
                String parentPath = path.substring(0, lastSlash);
                DataNode parent = nodes.get(parentPath);
                if (parent == null) {
                    throw new IOException("Invalid Datatree, unable to find " +
                            "parent " + parentPath + " of path " + path);
                }
                parent.addChild(path.substring(lastSlash + 1));
                long eowner = node.stat.getEphemeralOwner();
                if (eowner != 0) {
                    HashSet<String> list = ephemerals.get(eowner);
                    if (list == null) {
                        list = new HashSet<String>();
                        ephemerals.put(eowner, list);
                    }
                    list.add(path);
                }
            }
            path = ia.readString("path");
        }
        nodes.put("/", root);
        // we are done with deserializing the
        // the datatree
        // update the quotas - create path trie
        // and also update the stat nodes
        setupQuota();
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

    /**
     * Write a text dump of all the ephemerals in the datatree.
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        Set<Long> keys = ephemerals.keySet();
        pwriter.println("Sessions with Ephemerals ("
                + keys.size() + "):");
        for (long k : keys) {
            pwriter.print("0x" + Long.toHexString(k));
            pwriter.println(":");
            HashSet<String> tmp = ephemerals.get(k);
            synchronized (tmp) {
                for (String path : tmp) {
                    pwriter.println("\t" + path);
                }
            }
        }
    }

    public void removeCnxn(Watcher watcher) {
        dataWatches.removeWatcher(watcher);
        childWatches.removeWatcher(watcher);
    }

     /**
      * This method sets the Cversion and Pzxid for the specified node to the
      * values passed as arguments. The values are modified only if newCversion
      * is greater than the current Cversion. A NoNodeException is thrown if
      * a znode for the specified path is not found.
      *
      * @param path
      *     Full path to the znode whose Cversion needs to be modified.
      *     A "/" at the end of the path is ignored.
      * @param newCversion
      *     Value to be assigned to Cversion
      * @param zxid
      *     Value to be assigned to Pzxid
      * @throws KeeperException.NoNodeException
      *     If znode not found.
      **/
    public void setCversionPzxid(String path, int newCversion, long zxid)
        throws KeeperException.NoNodeException {
        if (path.endsWith("/")) {
           path = path.substring(0, path.length() - 1);
        }
        DataNode node = nodes.get(path);
        if (node == null) {
            throw new KeeperException.NoNodeException(path);
        }
        synchronized (node) {
            if(newCversion == -1) {
                newCversion = node.stat.getCversion() + 1;
            }
            if (newCversion > node.stat.getCversion()) {
                node.stat.setCversion(newCversion);
                node.stat.setPzxid(zxid);
            }
        }
    }
}
