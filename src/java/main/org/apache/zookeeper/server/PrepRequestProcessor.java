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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.apache.jute.BinaryOutputArchive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.AccessControlList;
import org.apache.zookeeper.common.Path;
import org.apache.zookeeper.common.ValidPath;
import org.apache.zookeeper.common.AccessControlList.Permission;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.server.Request.Meta;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 */
public class PrepRequestProcessor extends Thread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;
    static {
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    /**
     * this is only for testing purposes.
     * should never be useed otherwise
     */
    private static  boolean failCreate = false;

    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();

    private final RequestProcessor nextProcessor;

    ZooKeeperServer zks;

    public PrepRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {

        super("ProcessThread(sid:" + zks.getServerId()
                + " cport:" + zks.getClientPort() + "):");
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    /**
     * method for tests to set failCreate
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }
    @Override
    public void run() {
        try {
            while (true) {
                Request request = submittedRequests.take();

                if (Request.requestOfDeath == request) {
                    break;
                }
                pRequest(request);
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected interruption", e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    private ChangeRecord getRecordForPath(Path path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        if(path==null) {
            throw new KeeperException.NoNodeException("null");
        }
        synchronized (zks.outstandingChanges) {
            lastChange = zks.outstandingChangesForPath.get(path.toString());

            if (lastChange == null) {
                DataNode n = zks.getZKDatabase().getNode(path.toString());
                if (n != null) {
                    Long acl;
                    Set<String> children;
                    synchronized(n) {
                        acl = n.acl;
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path.toString(), n.stat,
                        children != null ? children.size() : 0,
                            zks.getZKDatabase().convertLong(acl));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    private void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     *
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     *
     * @param multiRequest
     */
    private Map<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();

        for(Op op: multiRequest) {
            String path = op.getPath();

            try {
                ChangeRecord cr = getRecordForPath(new Path(path));
                if (cr != null) {
                    pendingChangeRecords.put(path, cr);
                }
            } catch (KeeperException.NoNodeException e) {
                // ignore this one
            }
        }

        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords
     */
    void rollbackPendingChanges(long zxid, Map<String, ChangeRecord>pendingChangeRecords) {

        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            ListIterator<ChangeRecord> iter = zks.outstandingChanges.listIterator(zks.outstandingChanges.size());
            while (iter.hasPrevious()) {
                ChangeRecord c = iter.previous();
                if (c.zxid == zxid) {
                    iter.remove();
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }

            boolean empty = zks.outstandingChanges.isEmpty();
            long firstZxid = 0;
            if (!empty) {
                firstZxid = zks.outstandingChanges.get(0).zxid;
            }

            Iterator<ChangeRecord> priorIter = pendingChangeRecords.values().iterator();
            while (priorIter.hasNext()) {
                ChangeRecord c = priorIter.next();

                /* Don't apply any prior change records less than firstZxid */
                if (!empty && (c.zxid < firstZxid)) {
                    continue;
                }

                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type
     * @param zxid
     * @param request
     * @param record
     */
    protected Request pRequest2Txn(OpCode type, long zxid, Request request, Record record) throws KeeperException {
        Path path;
        Record txn = null;

        switch (type) {
            case create:
                CreateRequest createRequest = (CreateRequest)record;

                if(failCreate) throw new KeeperException.BadArgumentsException(createRequest.getPath());
                try {
                    path = new ValidPath(createRequest.getPath(),
                                         CreateMode.fromFlag(createRequest.getFlags()).isSequential());
                } catch(KeeperException.InvalidPathException e) {
                    LOG.info("Invalid path " + createRequest.getPath() + " with session 0x" +
                            Long.toHexString(request.getMeta().getSessionId()));
                    throw e;
                }

                AccessControlList acl = zks.accessControl.fixup(request.getMeta().getAuthInfo(),
                        AccessControlList.fromJuteACL(createRequest.getAcl()), path);
                ChangeRecord parentRecord = getRecordForPath(path.getParent());

                zks.accessControl.check(parentRecord.acl, Permission.CREATE, request.getMeta().getAuthInfo());
                int parentCVersion = parentRecord.stat.getCversion();
                CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
                if (createMode.isSequential()) {
                    path = path.appendSequenceNumber(parentCVersion);
                }
                try {
                    if (getRecordForPath(path) != null) {
                        throw new KeeperException.NodeExistsException(path);
                    }
                } catch (KeeperException.NoNodeException e) {
                    // ignore this one
                }
                boolean ephemeralParent = parentRecord.stat.getEphemeralOwner() != 0;
                if (ephemeralParent) {
                    throw new KeeperException.NoChildrenForEphemeralsException(path);
                }
                int newCversion = parentRecord.stat.getCversion()+1;
                txn = new CreateTxn(path.toString(), createRequest.getData(), acl.toJuteACL(),
                        createMode.isEphemeral(), newCversion);
                StatPersisted s = new StatPersisted();
                if (createMode.isEphemeral()) {
                    s.setEphemeralOwner(request.getMeta().getSessionId());
                }
                parentRecord = parentRecord.duplicate(zxid);
                parentRecord.childCount++;
                parentRecord.stat.setCversion(newCversion);
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(zxid, path.toString(), s, 0, acl));
                break;
            case delete:
                DeleteRequest deleteRequest = (DeleteRequest)record;
                path = new ValidPath(deleteRequest.getPath());

                if (zks.getZKDatabase().isSpecialPath(path.toString())) {
                    throw new KeeperException.BadArgumentsException(path.toString());
                }

                parentRecord = getRecordForPath(path.getParent());
                ChangeRecord nodeRecord = getRecordForPath(path);
                zks.accessControl.check(parentRecord.acl, Permission.DELETE, request.getMeta().getAuthInfo());
                checkAndIncVersion(nodeRecord.stat.getVersion(), deleteRequest.getVersion(), path);
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                txn = new DeleteTxn(path.toString());
                parentRecord = parentRecord.duplicate(zxid);
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(zxid, path.toString(), null, -1, null));
                break;
            case setData:
                SetDataRequest setDataRequest = (SetDataRequest)record;
                path = new Path(setDataRequest.getPath());
                nodeRecord = getRecordForPath(path);
                zks.accessControl.check(nodeRecord.acl, Permission.WRITE, request.getMeta().getAuthInfo());
                int newVersion = checkAndIncVersion(nodeRecord.stat.getVersion(), setDataRequest.getVersion(), path);
                txn = new SetDataTxn(path.toString(), setDataRequest.getData(), newVersion);
                nodeRecord = nodeRecord.duplicate(zxid);
                nodeRecord.stat.setVersion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            case setACL:
                SetACLRequest setAclRequest = (SetACLRequest)record;
                path = new Path(setAclRequest.getPath());
                acl = zks.accessControl.fixup(request.getMeta().getAuthInfo(),
                        AccessControlList.fromJuteACL(setAclRequest.getAcl()), path);
                nodeRecord = getRecordForPath(path);
                zks.accessControl.check(nodeRecord.acl, Permission.ADMIN, request.getMeta().getAuthInfo());
                newVersion = checkAndIncVersion(nodeRecord.stat.getAversion(), setAclRequest.getVersion(), path);
                txn = new SetACLTxn(path.toString(), acl.toJuteACL(), newVersion);
                nodeRecord = nodeRecord.duplicate(zxid);
                nodeRecord.stat.setAversion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            case createSession:
                request.request.rewind();
                int to = request.request.getInt();
                txn = new CreateSessionTxn(to);
                request.request.rewind();
                zks.sessionTracker.addSession(request.getMeta().getSessionId(), to);
                zks.setOwner(request.getMeta().getSessionId(), request.getMeta().getOwner());
                break;
            case closeSession:
                // We don't want to do this check since the session expiration thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                //zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                Set<String> es = zks.getZKDatabase()
                        .getEphemerals(request.getMeta().getSessionId());
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.getMeta().getSessionId()) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(zxid, path2Delete, null, 0, null));
                    }
                }
                LOG.info("Processed session termination for sessionid: 0x"
                        + Long.toHexString(request.getMeta().getSessionId()));
                break;
            case check:
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest)record;
                path = new Path(checkVersionRequest.getPath());
                nodeRecord = getRecordForPath(path);
                zks.accessControl.check(nodeRecord.acl, Permission.READ, request.getMeta().getAuthInfo());
                txn = new CheckVersionTxn(path.toString(), checkAndIncVersion(nodeRecord.stat.getVersion(),
                        checkVersionRequest.getVersion(), path));
                break;
        }
        TxnHeader hdr = new TxnHeader(request.getMeta().getSessionId(), request.getMeta().getCxid(), zxid, zks.getTime(), type.getInt());
        return new Request(request.getMeta().cloneWithZxid(zxid), request.request, hdr, txn);
    }

    private static int checkAndIncVersion(int currentVersion, int expectedVersion, Path path)
            throws KeeperException.BadVersionException {
        if (expectedVersion != -1 && expectedVersion != currentVersion) {
            throw new KeeperException.BadVersionException(path);
        }
        return currentVersion + 1;
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    protected void pRequest(Request request) {
        if(request.getHdr() != null) throw new RuntimeException("expected request hdr to be null: " + request);
        if(request.getTxn() != null) throw new RuntimeException("expected request txn to be null: " + request);
        Request newRequest = request;

        try {
            if(request.getMeta().getType() != OpCode.createSession && request.getMeta().getType() != OpCode.closeSession) {
                zks.sessionTracker.checkSession(request.getMeta().getSessionId(), request.getMeta().getOwner());
            }

            switch (request.getMeta().getType()) {
            case create:
            case delete:
            case setData:
            case setACL:
            case check:
                newRequest = pRequest2Txn(request.getMeta().getType(), zks.getNextZxid(), request, request.deserializeRequestRecord());
                break;
            case multi:
                MultiTransactionRecord multiRequest = (MultiTransactionRecord) request.deserializeRequestRecord();
                List<Txn> txns = new ArrayList<Txn>();

                //Each op in a multi-op must have the same zxid!
                long zxid = zks.getNextZxid();
                KeeperException ke = null;

                //Store off current pending change records in case we need to rollback
                Map<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                for(Op op: multiRequest) {
                    Record subrequest = op.toRequestRecord();
                    OpCode type;
                    Record txn;

                    /* If we've already failed one of the ops, don't bother
                     * trying the rest as we know it's going to fail and it
                     * would be confusing in the logfiles.
                     */
                    if (ke != null) {
                        type = OpCode.error;
                        txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                    }

                    /* Prep the request and convert to a Txn */
                    else {
                        try {
                            Request subRequest = pRequest2Txn(op.getType(), zxid, request, subrequest);
                            type = op.getType();
                            txn = subRequest.getTxn();
                        } catch (KeeperException e) {
                            ke = e;
                            type = OpCode.error;
                            txn = new ErrorTxn(e.code().intValue());
                            LOG.error(">>>> Got user-level KeeperException when processing "
                                    + request.toString()
                                    + " Error Path:" + e.getPath()
                                    + " Error:" + e.getMessage());
                            LOG.error(">>>> ABORTING remaing MultiOp ops");

                            /* Rollback change records from failed multi-op */
                            rollbackPendingChanges(zxid, pendingChanges);
                        }
                    }

                    //FIXME: I don't want to have to serialize it here and then
                    //       immediately deserialize in next processor. But I'm
                    //       not sure how else to get the txn stored into our list.
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                    txn.serialize(boa, "request") ;
                    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                    txns.add(new Txn(type.getInt(), bb.array()));
                }

                newRequest = new Request(request.getMeta().cloneWithZxid(zxid), request.request,
                        new TxnHeader(request.getMeta().getSessionId(), request.getMeta().getCxid(),
                                zxid, zks.getTime(), request.getMeta().getType().getInt()),
                        new MultiTxn(txns)
                );
                break;

            //create/close session don't require request record
            case createSession:
            case closeSession:
                newRequest = pRequest2Txn(request.getMeta().getType(), zks.getNextZxid(), request, null);
                break;
            }
        } catch (KeeperException e) {
            newRequest = createErrorRequest(request, e.code(), zks.getTime(), zks.getZxid());
            LOG.info("Got user-level KeeperException when processing "
                    + request.toString()
                    + " Error Path:" + e.getPath()
                    + " Error:" + e.getMessage());
            newRequest.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if(bb != null){
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            newRequest = createErrorRequest(request, Code.MARSHALLINGERROR, zks.getTime(), zks.getZxid());
        }
        nextProcessor.processRequest(newRequest);
    }

    private static final Request createErrorRequest(Request request, Code errorCode, long time, long zxid) {
        if (!request.getMeta().getType().isWriteOp()) {
            return request;
        }
        Meta meta = request.getMeta().cloneWithZxid(zxid);
        TxnHeader hdr = new TxnHeader(meta.getSessionId(), meta.getCxid(), zxid, time, OpCode.error.getInt());
        Record txn = new ErrorTxn(errorCode.intValue());
        return new Request(meta, request.request, hdr, txn);
    }

    public void processRequest(Request request) {
        submittedRequests.add(request);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}
