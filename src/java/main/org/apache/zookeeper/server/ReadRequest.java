package org.apache.zookeeper.server;

import java.util.ArrayList;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.common.AccessControlList;
import org.apache.zookeeper.common.AccessControlList.Permission;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.server.Request.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReadRequest {
    static final Logger LOG = LoggerFactory.getLogger(ReadRequest.class);

    protected final String path;
    protected final boolean watch;

    protected ReadRequest(String path, boolean watch) {
        this.path = path;
        this.watch = watch;
    }

    abstract Record getResponse(DataNode node);
    abstract void setWatcher(DataTree tree, Watcher watcher);
    void checkPermission(ZooKeeperServer zks, Request request, DataNode node) throws NoAuthException {
        AccessControlList acl = zks.getZKDatabase().convertLong(node.acl);
        zks.accessControl.check(acl, Permission.READ, request.getMeta().getAuthInfo());
    }

    Record process(ZooKeeperServer zks, Request request) throws NoNodeException, NoAuthException {
        DataTree tree = zks.getZKDatabase().getDataTree();
        DataNode node = tree.getNode(path);
        if(node == null) throw new KeeperException.NoNodeException(path);
        checkPermission(zks, request, node);
        if(watch) setWatcher(tree, request.getMeta().getCnxn());
        return getResponse(node);
    }

    static ReadRequest tryFromRecord(Record record, Meta meta) throws KeeperException {
        switch(meta.getType()) {
        case exists: return new ReadRequest.Exists((ExistsRequest) record);
        case getData: return new ReadRequest.GetData((GetDataRequest) record);
        case getACL: return new ReadRequest.GetACL((GetACLRequest) record);
        case getChildren: return new ReadRequest.GetChildren((GetChildrenRequest) record);
        case getChildren2: return new ReadRequest.GetChildren2((GetChildren2Request) record);
        }
        throw new RuntimeException("unknown type " + meta.getType());
    }

    public static class GetChildren2 extends ReadRequest {
        public GetChildren2(GetChildren2Request record) {
            super(record.getPath(), record.getWatch());
        }

        @Override
        Record getResponse(DataNode node) {
            ArrayList<String> children;
            Set<String> childs = node.getChildren();
            if (childs == null) {
                children = new ArrayList<String>(0);
            } else {
                children = new ArrayList<String>(childs);
            }
            return new GetChildren2Response(children, node.getStat());
        }

        @Override
        void setWatcher(DataTree tree, Watcher watcher) {
            tree.childWatches.addWatch(path, watcher);
        }

    }

    public static class GetChildren extends ReadRequest {
        public GetChildren(GetChildrenRequest record) {
            super(record.getPath(), record.getWatch());
        }

        @Override
        Record getResponse(DataNode node) {
            ArrayList<String> children;
            Set<String> childs = node.getChildren();
            if (childs == null) {
                children = new ArrayList<String>(0);
            } else {
                children = new ArrayList<String>(childs);
            }
            return new GetChildrenResponse(children);
        }

        @Override
        void setWatcher(DataTree tree, Watcher watcher) {
            tree.childWatches.addWatch(path, watcher);
        }
    }

    public static class GetData extends ReadRequest {
        public GetData(GetDataRequest record) {
            super(record.getPath(), record.getWatch());
        }

        @Override
        Record getResponse(DataNode node) {
            return new GetDataResponse(node.data, node.getStat());
        }

        @Override
        void setWatcher(DataTree tree, Watcher watcher) {
            tree.dataWatches.addWatch(path, watcher);
        }
    }

    public static class GetACL extends ReadRequest {
        public GetACL(GetACLRequest record) {
            super(record.getPath(), false);
        }

        @Override Record getResponse(DataNode node) { throw new RuntimeException(); }
        @Override void setWatcher(DataTree tree, Watcher watcher) { throw new RuntimeException(); }

        @Override
        Record process(ZooKeeperServer zks, Request request) throws NoNodeException, NoAuthException {
            DataTree tree = zks.getZKDatabase().getDataTree();
            DataNode node = tree.getNode(path);
            if(node == null) throw new KeeperException.NoNodeException(path);
            return new GetACLResponse(tree.convertLong(node.acl).toJuteACL(), node.getStat());
        }
    }

    public static class Exists extends ReadRequest {
        public Exists(ExistsRequest record) {
            super(record.getPath(), record.getWatch());
        }

        @Override Record getResponse(DataNode node) { throw new RuntimeException(); }

        @Override
        Record process(ZooKeeperServer zks, Request request) throws NoNodeException {
            DataTree tree = zks.getZKDatabase().getDataTree();
            if(watch) setWatcher(tree, request.getMeta().getCnxn());

            DataNode node = tree.getNode(path);
            if(node == null) {
                throw new KeeperException.NoNodeException();
            }

            return new ExistsResponse(node.getStat());
        }

        @Override void setWatcher(DataTree tree, Watcher watcher) {
            tree.dataWatches.addWatch(path, watcher);
        }
    }
}
