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
import org.apache.zookeeper.common.Path;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Transaction {
    private static final Logger LOG = LoggerFactory.getLogger(Transaction.class);

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

    public static Transaction fromTxn(TxnHeader header, Record txn) {
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

    ////////////////////////////////////
    // Interfaces
    ////////////////////////////////////

    public abstract static class PathTransaction extends Transaction {
        protected final Path path;

        protected PathTransaction(TxnHeader header, String path) {
            super(header);
            this.path = new Path(path);
        }

        public Path getPath() { return path; }
    }

    public interface TriggerWatches {
        public void triggerWatches(DataTree tree);
    }
    ///////////////////////////////////
    // Transaction sub classes
    ///////////////////////////////////

    public static final class Create extends PathTransaction implements TriggerWatches {
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

        @Override public Record getResponse(ProcessTxnResult rc) { return new CreateResponse(path.toString()); }

        @Override
        public ProcessTxnResult process(DataTree tree) throws NodeExistsException, NoNodeException {
            long ephemeralOwner = ephemeral ? clientId : 0;
            String parentName = path.getParent().toString();
            String childName = path.basename().toString();
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
                tree.nodes.put(path.toString(), child);
                if (ephemeralOwner != 0) {
                    HashSet<String> list = tree.ephemerals.get(ephemeralOwner);
                    if (list == null) {
                        list = new HashSet<String>();
                        tree.ephemerals.put(ephemeralOwner, list);
                    }
                    synchronized (list) {
                        list.add(path.toString());
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
            return new ProcessTxnResult(OpCode.create, path.toString(), null, this);
        }

        @Override
        public void triggerWatches(DataTree tree) {
            tree.dataWatches.triggerWatch(path, Event.EventType.NodeCreated);
            tree.childWatches.triggerWatch(path.getParent(), Event.EventType.NodeChildrenChanged);
        }
    }

    public static final class Delete extends PathTransaction implements TriggerWatches {

        public Delete(TxnHeader header, DeleteTxn txn) {
            super(header, txn.getPath());
        }

        @Override public Record getResponse(ProcessTxnResult rc) { return null; }

        @Override
        public ProcessTxnResult process(DataTree tree) throws NoNodeException {
            String parentName = path.getParent().toString();
            String childName = path.basename().toString();
            DataNode node = tree.nodes.get(path.toString());
            if (node == null) {
                throw new KeeperException.NoNodeException();
            }
            tree.nodes.remove(path.toString());
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
                            nodes.remove(path.toString());
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

            return new ProcessTxnResult(OpCode.delete, path.toString(), null, this);
        }

        @Override
        public void triggerWatches(DataTree tree) {
            Set<Watcher> processed = tree.dataWatches.triggerWatch(path, EventType.NodeDeleted);
            tree.childWatches.triggerWatch(path, EventType.NodeDeleted, processed);
            tree.childWatches.triggerWatch(path.getParent(), EventType.NodeChildrenChanged);
        }
    }

    public static final class SetData extends PathTransaction implements TriggerWatches {
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
            Stat s;
            DataNode n = tree.nodes.get(path.toString());
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
                s = n.getStat();
            }
            // now update if the path is in a quota subtree.
            String lastPrefix = tree.getMaxPrefixWithQuota(path);
            if(lastPrefix != null) {
              tree.updateBytes(lastPrefix, (data == null ? 0 : data.length)
                  - (lastdata == null ? 0 : lastdata.length));
            }

            return new ProcessTxnResult(OpCode.setData, path.toString(), s, this);
        }

        @Override
        public void triggerWatches(DataTree tree) {
            tree.dataWatches.triggerWatch(path, EventType.NodeDataChanged);
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
            DataNode n = tree.nodes.get(path.toString());
            if (n == null) {
                throw new KeeperException.NoNodeException();
            }
            synchronized (n) {
                n.stat.setAversion(version);
                n.acl = tree.convertAcls(acl);
                return new ProcessTxnResult(OpCode.setACL, path.toString(), n.getStat());
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
            return new ProcessTxnResult(OpCode.check, path.toString(), null);
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
                return new MultiResponse(((MultiTxnResult)rc).multiResult) ;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ProcessTxnResult process(DataTree tree) {
            List<ProcessTxnResult> multiResult = new ArrayList<ProcessTxnResult>();
            List<TriggerWatches> trigger = new ArrayList<TriggerWatches>(transactions.size());

            boolean postFailed = false;
            for (Transaction subtxn : transactions) {
                OpCode type = subtxn.type;
                if(type == OpCode.error) {
                    postFailed = true;
                }

                ProcessTxnResult subRc;
                if (error != Code.OK && type != OpCode.error){
                    subRc = new ProcessTxnResult(postFailed ? Code.RUNTIMEINCONSISTENCY.intValue()
                                                            : Code.OK.intValue(), null);
                } else {
                    try {
                        subRc = subtxn.process(tree);
                        trigger.add(subRc.trigger);
                    } catch (KeeperException e) {
                        LOG.debug("SubTxn of type {} failed: {}", type.longString, e);
                        subRc = null;
                    }
                }

                multiResult.add(subRc);
            }
            return new MultiTxnResult(multiResult, trigger);
        }

        private static class MultiTxnResult extends ProcessTxnResult {
            public List<ProcessTxnResult> multiResult;

            public MultiTxnResult(List<ProcessTxnResult> multiResult, List<TriggerWatches> triggerList) {
                super(OpCode.multi, 0, null, null, new MultiTriggerWatches(triggerList));
                this.multiResult = multiResult;
            }
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
            // the list is already removed from the ephemerals
            // so we do not have to worry about synchronizing on
            // the list. This is only called from FinalRequestProcessor
            // so there is no need for synchronization. The list is not
            // changed here. Only create and delete change the list which
            // are again called from FinalRequestProcessor in sequence.
            HashSet<String> list = tree.ephemerals.remove(clientId);
            List<TriggerWatches> trigger = new ArrayList<TriggerWatches>(list != null ? list.size() : 0);
            TxnHeader header = new TxnHeader();
            header.setZxid(zxid);
            if (list != null) {
                for (String path : list) {
                    try {
                        DeleteTxn delTxn = new DeleteTxn(path);
                        Transaction.Delete transaction = new Transaction.Delete(header, delTxn);
                        ProcessTxnResult rc = transaction.process(tree);
                        trigger.add(rc.trigger);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Deleting ephemeral node " + path
                                            + " for session 0x"
                                            + Long.toHexString(clientId));
                        }
                    } catch (NoNodeException e) {
                        LOG.warn("Ignoring NoNodeException for path " + path
                                + " while removing ephemeral for dead session 0x"
                                + Long.toHexString(clientId));
                    }
                }
            }
            return new ProcessTxnResult(OpCode.closeSession, null, null, new MultiTriggerWatches(trigger));
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
            return new ProcessTxnResult(errorCode.intValue(), null);
        }
    }

    static public class ProcessTxnResult {
        private static final TriggerWatches NO_OP_TRIGGER = new TriggerWatches(){
            @Override public void triggerWatches(DataTree t){}
        };

        public final int err;

        public final OpCode type;

        public final String path;

        public final Stat stat;

        public final TriggerWatches trigger;

        public ProcessTxnResult(int err, String path) {
            this.type = OpCode.error;
            this.path = path;
            this.stat = null;
            this.trigger = NO_OP_TRIGGER;
            this.err = err;
        }

        private ProcessTxnResult(OpCode type, String path, Stat stat) {
            this(type, path, stat, NO_OP_TRIGGER);
        }

        private ProcessTxnResult(OpCode type, String path, Stat stat, TriggerWatches trigger) {
            this.err = 0;
            this.type = type;
            this.path = path;
            this.stat = stat;
            this.trigger = trigger;
        }

        private ProcessTxnResult(OpCode type, int err, String path, Stat stat, TriggerWatches trigger) {
            this.err = err;
            this.type = type;
            this.path = path;
            this.stat = stat;
            this.trigger = trigger;
        }
    }

    static private class MultiTriggerWatches implements TriggerWatches {
        private final List<TriggerWatches> triggerList;

        private MultiTriggerWatches(List<TriggerWatches> triggerList) {
            this.triggerList = triggerList;
        }

        @Override
        public void triggerWatches(DataTree tree) {
            for(TriggerWatches trigger : triggerList) {
                trigger.triggerWatches(tree);
            }
        }
    }
}
