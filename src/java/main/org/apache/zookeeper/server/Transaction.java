package org.apache.zookeeper.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.AccessControlList;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;

public abstract class Transaction {
    protected final long clientId;
    protected final int cxid;
    protected final long zxid;
    protected final long time;
    protected final OpCode type;

    private static final String quotaZookeeper = Quotas.quotaZookeeper;
    private static final String procZookeeper = Quotas.procZookeeper;

    // Nobody else should be able to create a Transaction instance
    private Transaction(TxnHeader header) {
        this.clientId = header.getClientId();
        this.cxid = header.getCxid();
        this.zxid = header.getZxid();
        this.time = header.getTime();
        this.type = OpCode.fromInt(header.getType());
    }

    static Transaction fromTxn(TxnHeader header, Record txn) {
        switch (OpCode.fromInt(header.getType())) {
        case create: return new Create(header, (CreateTxn)txn);
        case delete: return new Delete(header, (DeleteTxn)txn);
        case setData: return new SetData(header, (SetDataTxn)txn);
        case setACL: return new SetACL(header, (SetACLTxn)txn);
        case createSession: return new CreateSession(header);
        case closeSession: return new CloseSession(header);
        case error: return new Error(header, (ErrorTxn)txn);
        case check: return new Check(header, (CheckVersionTxn)txn);
        case multi:
            try {
                return new MultiTransaction(header, (MultiTxn)txn);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        default: throw new RuntimeException("No Transaction for OpCode type " + header.getType());
        }
    }

    public abstract Record getResponse(ProcessTxnResult rc);

    public abstract ProcessTxnResult process(DataTree tree) throws KeeperException;

    public abstract static class PathTransaction extends Transaction {
        protected final String path;

        protected PathTransaction(TxnHeader header, String path) {
            super(header);
            this.path = path;
        }
    }

    public static final class Create extends PathTransaction {
        private final byte[] data;
        private final AccessControlList acl;
        private final boolean ephemeral;
        private final int parentCVersion;

        public Create(TxnHeader header, CreateTxn txn) {
            super(header, txn.getPath());
            this.data = txn.getData();
            this.acl = AccessControlList.fromJuteACL(txn.getAcl());
            this.ephemeral = txn.getEphemeral();
            this.parentCVersion = txn.getParentCVersion();
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return new CreateResponse(path); }

        @Override
        public ProcessTxnResult process(DataTree tree) throws NodeExistsException, NoNodeException {
            long ephemeralOwner = ephemeral ? clientId : 0;
            int lastSlash = path.lastIndexOf('/');
            String parentName = path.substring(0, lastSlash);
            String childName = path.substring(lastSlash + 1);
            StatPersisted stat = new StatPersisted();
            stat.setCtime(time);
            stat.setMtime(time);
            stat.setCzxid(zxid);
            stat.setMzxid(zxid);
            stat.setPzxid(zxid);
            stat.setVersion(0);
            stat.setAversion(0);
            stat.setEphemeralOwner(ephemeralOwner);
            DataNode parent = tree.nodes.get(parentName);
            if (parent == null) {
                throw new KeeperException.NoNodeException();
            }
            synchronized (parent) {
                Set<String> children = parent.getChildren();
                if (children != null && children.contains(childName)) {
                    throw new KeeperException.NodeExistsException();
                }

                parent.stat.setCversion(parentCVersion == -1 ? parent.stat.getCversion() + 1 : parentCVersion);
                parent.stat.setPzxid(zxid);
                Long longval = tree.convertAcls(acl);
                DataNode child = new DataNode(data, longval, stat);
                parent.addChild(childName);
                tree.nodes.put(path, child);
                if (ephemeralOwner != 0) {
                    HashSet<String> list = tree.ephemerals.get(ephemeralOwner);
                    if (list == null) {
                        list = new HashSet<String>();
                        tree.ephemerals.put(ephemeralOwner, list);
                    }
                    synchronized (list) {
                        list.add(path);
                    }
                }
            }
            // now check if its one of the zookeeper node child
            if (parentName.startsWith(quotaZookeeper)) {
                // now check if its the limit node
                if (Quotas.limitNode.equals(childName)) {
                    // this is the limit node
                    // get the parent and add it to the trie
                    tree.pTrie.addPath(parentName.substring(quotaZookeeper.length()));
                }
                if (Quotas.statNode.equals(childName)) {
                    tree.updateQuotaForPath(parentName
                            .substring(quotaZookeeper.length()));
                }
            }
            // also check to update the quotas for this node
            String lastPrefix = tree.getMaxPrefixWithQuota(path);
            if(lastPrefix != null) {
                // ok we have some match and need to update
                tree.updateCount(lastPrefix, 1);
                tree.updateBytes(lastPrefix, data == null ? 0 : data.length);
            }
            tree.dataWatches.triggerWatch(path, Event.EventType.NodeCreated);
            tree.childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                    Event.EventType.NodeChildrenChanged);
            return new ProcessTxnResult(OpCode.create, path, null);
        }
    }

    public static final class Delete extends PathTransaction {

        public Delete(TxnHeader header, DeleteTxn txn) {
            super(header, txn.getPath());
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return null; }

        @Override
        public ProcessTxnResult process(DataTree tree) throws NoNodeException {
            int lastSlash = path.lastIndexOf('/');
            String parentName = path.substring(0, lastSlash);
            String childName = path.substring(lastSlash + 1);
            DataNode node = tree.nodes.get(path);
            if (node == null) {
                throw new KeeperException.NoNodeException();
            }
            tree.nodes.remove(path);
            DataNode parent = tree.nodes.get(parentName);
            if (parent == null) {
                throw new KeeperException.NoNodeException();
            }
            synchronized (parent) {
                parent.removeChild(childName);
                parent.stat.setPzxid(zxid);
                long eowner = node.stat.getEphemeralOwner();
                if (eowner != 0) {
                    HashSet<String> nodes = tree.ephemerals.get(eowner);
                    if (nodes != null) {
                        synchronized (nodes) {
                            nodes.remove(path);
                        }
                    }
                }
            }
            if (parentName.startsWith(procZookeeper) && Quotas.limitNode.equals(childName)) {
                // delete the node in the trie.
                // we need to update the trie as well
                tree.pTrie.deletePath(parentName.substring(quotaZookeeper.length()));
            }

            // also check to update the quotas for this node
            String lastPrefix = tree.getMaxPrefixWithQuota(path);
            if(lastPrefix != null) {
                // ok we have some match and need to update
                tree.updateCount(lastPrefix, -1);
                int bytes = 0;
                synchronized (node) {
                    bytes = (node.data == null ? 0 : -(node.data.length));
                }
                tree.updateBytes(lastPrefix, bytes);
            }

            Set<Watcher> processed = tree.dataWatches.triggerWatch(path,
                    EventType.NodeDeleted);
            tree.childWatches.triggerWatch(path, EventType.NodeDeleted, processed);
            tree.childWatches.triggerWatch("".equals(parentName) ? "/" : parentName,
                    EventType.NodeChildrenChanged);
            return new ProcessTxnResult(OpCode.delete, path, null);
        }
    }

    public static final class SetData extends PathTransaction {
        private final int version;
        private final byte[] data;

        public SetData(TxnHeader header, SetDataTxn txn) {
            super(header, txn.getPath());
            data = txn.getData();
            version = txn.getVersion();
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return new SetDataResponse(rc.stat); }

        @Override
        public ProcessTxnResult process(DataTree tree) throws NoNodeException {
            Stat s = new Stat();
            DataNode n = tree.nodes.get(path);
            if (n == null) {
                throw new KeeperException.NoNodeException();
            }
            byte lastdata[] = null;
            synchronized (n) {
                lastdata = n.data;
                n.data = data;
                n.stat.setMtime(time);
                n.stat.setMzxid(zxid);
                n.stat.setVersion(version);
                n.copyStat(s);
            }
            // now update if the path is in a quota subtree.
            String lastPrefix = tree.getMaxPrefixWithQuota(path);
            if(lastPrefix != null) {
              tree.updateBytes(lastPrefix, (data == null ? 0 : data.length)
                  - (lastdata == null ? 0 : lastdata.length));
            }
            tree.dataWatches.triggerWatch(path, EventType.NodeDataChanged);
            return new ProcessTxnResult(OpCode.setData, path, s);
        }
    }

    public static final class SetACL extends PathTransaction {
        private final int version;
        private final AccessControlList acl;

        public SetACL(TxnHeader header, SetACLTxn txn) {
            super(header, txn.getPath());
            acl =  AccessControlList.fromJuteACL(txn.getAcl());
            version = txn.getVersion();
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return new SetACLResponse(rc.stat); }

        @Override
        public ProcessTxnResult process(DataTree tree) throws NoNodeException {
            Stat stat = new Stat();
            DataNode n = tree.nodes.get(path);
            if (n == null) {
                throw new KeeperException.NoNodeException();
            }
            synchronized (n) {
                n.stat.setAversion(version);
                n.acl = tree.convertAcls(acl);
                n.copyStat(stat);
                return new ProcessTxnResult(OpCode.setACL, path, stat);
            }
        }
    }

    public static final class Check extends PathTransaction {
        private final int version;

        public Check(TxnHeader header, CheckVersionTxn txn) {
            super(header, txn.getPath());
            version = txn.getVersion();
        }

        @Override public Record getResponse(ProcessTxnResult rc) {
            return new SetDataResponse(rc.stat); // yes, the response class is reused!
        }

        @Override
        public ProcessTxnResult process(DataTree tree) {
            return new ProcessTxnResult(OpCode.check, path, null);
        }
    }

    public static final class MultiTransaction extends Transaction {
        private final List<Transaction> transactions = new ArrayList<Transaction>();
        private final Code error;

        public MultiTransaction(TxnHeader header, MultiTxn txn) throws IOException {
            super(header);
            Code error = Code.OK;
            for(Txn subTxn: txn.getTxns()) {
                OpCode type = OpCode.fromInt(subTxn.getType());

                Record record = SerializeUtils.getRecordForType(type);
                ByteBufferInputStream.byteBuffer2Record(ByteBuffer.wrap(subTxn.getData()), record);
                // subHeader has only type changed
                TxnHeader subHeader = new TxnHeader(clientId, cxid, zxid, time, type.getInt());
                Transaction subTransaction = fromTxn(subHeader, record);
                transactions.add(subTransaction);

                if(error == Code.OK && type == OpCode.error) error = ((Error)subTransaction).errorCode;
            }
            this.error = error;
        }

        @Override
        public Record getResponse(ProcessTxnResult rc) {
            try {
                return new MultiResponse(rc.multiResult) ;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ProcessTxnResult process(DataTree tree) throws KeeperException {
            List<ProcessTxnResult> multiResult = new ArrayList<ProcessTxnResult>();

            boolean postFailed = false;
            for (Transaction subtxn : transactions) {
                OpCode type = subtxn.type;
                if(type == OpCode.error) {
                    postFailed = true;
                }

                ProcessTxnResult subRc;
                if (error != Code.OK && type != OpCode.error){
                    subRc = new ProcessTxnResult(postFailed ? Code.RUNTIMEINCONSISTENCY.intValue()
                                                            : Code.OK.intValue());
                } else {
                    subRc = subtxn.process(tree);
                }

                multiResult.add(subRc);
            }
            ProcessTxnResult result = new ProcessTxnResult(error.intValue());
            result.multiResult = multiResult;
            result.type = OpCode.multi;
            return result;
        }
    }

    public static class CreateSession extends Transaction {
        public CreateSession(TxnHeader header) {
            super(header);
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return null; }

        @Override
        public ProcessTxnResult process(DataTree tree) {
            return new ProcessTxnResult(OpCode.createSession, null, null);
        }
    }

    public static class CloseSession extends Transaction {
        public CloseSession(TxnHeader header) {
            super(header);
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return null; }

        @Override
        public ProcessTxnResult process(DataTree tree) {
            tree.killSession(clientId, zxid);
            return new ProcessTxnResult(OpCode.closeSession, null, null);
        }
    }

    public static final class Error extends Transaction {
        final private Code errorCode;

        public Error(TxnHeader header, ErrorTxn txn) {
            super(header);
            errorCode = Code.get(txn.getErr());
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return new ErrorTxn(errorCode.intValue()); }

        @Override
        public ProcessTxnResult process(DataTree tree) {
            return new ProcessTxnResult(errorCode.intValue());
        }
    }
}
