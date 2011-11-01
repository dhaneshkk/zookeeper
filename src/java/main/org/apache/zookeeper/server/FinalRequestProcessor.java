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
import java.nio.ByteBuffer;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.Request.Meta;
import org.apache.zookeeper.server.Transaction.PathTransaction;
import org.apache.zookeeper.server.Transaction.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Request processor actually applies any transaction associated with a
 * request and services any queries. It is always at the end of a
 * RequestProcessor chain (hence the name), so it does not have a nextProcessor
 * member.
 *
 * This RequestProcessor counts on ZooKeeperServer to populate the
 * outstandingRequests member of ZooKeeperServer.
 */
public class FinalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FinalRequestProcessor.class);

    ZooKeeperServer zks;

    public FinalRequestProcessor(ZooKeeperServer zks) {
        this.zks = zks;
    }

    public void processRequest(Request request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }

        Transaction transaction = null;
        ProcessTxnResult rc = null;
        synchronized (zks.outstandingChanges) {
            while (!zks.outstandingChanges.isEmpty()
                    && zks.outstandingChanges.get(0).zxid <= request.getMeta().getZxid()) {
                ChangeRecord cr = zks.outstandingChanges.remove(0);
                if (cr.zxid < request.getMeta().getZxid()) {
                    LOG.warn("Zxid outstanding {} is less than current {}", cr.zxid, request.getMeta().getZxid());
                }
                if (zks.outstandingChangesForPath.get(cr.path) == cr) {
                    zks.outstandingChangesForPath.remove(cr.path);
                }
            }
            if (request.getHdr() != null) {
                transaction = Transaction.fromTxn(request.getHdr(), request.getTxn());
                DataTree tree = zks.getZKDatabase().getDataTree();
                try {
                    rc = transaction.process(tree);
                    rc.trigger.triggerWatches(tree);
                } catch (KeeperException e) {
                    rc = new ProcessTxnResult(e.code().intValue(),
                                transaction instanceof PathTransaction
                                    ? ((PathTransaction)transaction).getPath().toString()
                                    : null
                            );
                    LOG.warn("Failed: ", e);
                }
                /*
                 * A snapshot might be in progress while we are modifying the data
                 * tree. If we set lastProcessedZxid prior to making corresponding
                 * change to the tree, then the zxid associated with the snapshot
                 * file will be ahead of its contents. Thus, while restoring from
                 * the snapshot, the restore method will not apply the transaction
                 * for zxid associated with the snapshot file, since the restore
                 * method assumes that transaction to be present in the snapshot.
                 *
                 * To avoid this, we first apply the transaction and then modify
                 * lastProcessedZxid.  During restore, we correctly handle the
                 * case where the snapshot contains data ahead of the zxid associated
                 * with the file.
                 */
                if (request.getHdr().getZxid() > tree.lastProcessedZxid) {
                    tree.lastProcessedZxid = request.getHdr().getZxid();
                }
                if (request.getMeta().getType() == OpCode.createSession) {
                    if (request.getTxn() instanceof CreateSessionTxn) {
                        CreateSessionTxn cst = (CreateSessionTxn) request.getTxn();
                        zks.sessionTracker.addSession(request.getMeta().getSessionId(), cst.getTimeOut());
                    } else {
                        LOG.warn("*****>>>>> Got {} {}", request.getTxn().getClass(), request.getTxn());
                    }
                } else if (request.getMeta().getType() == OpCode.closeSession) {
                    zks.sessionTracker.removeSession(request.getMeta().getSessionId());
                }
                zks.getZKDatabase().addCommittedProposal(request);
            }
        }

        if (request.getHdr() != null && OpCode.closeSession.is(request.getHdr().getType())) {
            ServerCnxnFactory scxn = zks.getServerCnxnFactory();
            // this might be possible since
            // we might just be playing diffs from the leader
            if (scxn != null && request.getMeta().getCnxn() == null) {
                // calling this if we have the cnxn results in the client's
                // close session response being lost - we've already closed
                // the session/socket here before we can send the closeSession
                // in the switch block below
                scxn.closeSession(request.getMeta().getSessionId());
                return;
            }
        }

        if (request.getMeta().getCnxn() == null) {
            return;
        }
        ServerCnxn cnxn = request.getMeta().getCnxn();

        zks.decInProcess();
        Code err = Code.OK;
        Record rsp = null;
        try {

            if (request.getHdr() != null && OpCode.error.is(request.getHdr().getType())) {
                throw KeeperException.create(KeeperException.Code.get((
                        (ErrorTxn) request.getTxn()).getErr()));
            }

            if (request.getException() != null) {
                throw request.getException();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("{}",request);
            }
            switch (request.getMeta().getType()) {
            case ping: {
                updateStats(request.getMeta());
                cnxn.sendResponse(new ReplyHeader(-2,
                        zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null, "response");
                return;
            }
            case createSession: {
                updateStats(request.getMeta());
                zks.finishSessionInit(request.getMeta().getCnxn(), true);
                return;
            }
            case multi:
            case create:
            case delete:
            case setData:
            case setACL:
            case closeSession:
            case check: {
                rsp = transaction.getResponse(rc);
                err = Code.get(rc.err);
                break;
            }
            case sync: {
                SyncRequest syncRequest = (SyncRequest)request.deserializeRequestRecord();
                rsp = new SyncResponse(syncRequest.getPath());
                break;
            }

            case setWatches:
                // XXX We really should NOT need this!!!!
                // request.getOriginalByteBuffer().rewind();
            case exists:
            case getData:
            case getACL:
            case getChildren:
            case getChildren2:
                rsp = ReadRequest.tryFromRecord(request.deserializeRequestRecord(), request.getMeta())
                        .process(zks, request);
                break;
            }
        } catch (SessionMovedException e) {
            // session moved is a connection level error, we need to tear
            // down the connection otw ZOOKEEPER-710 might happen
            // ie client on slow follower starts to renew session, fails
            // before this completes, then tries the fast follower (leader)
            // and is successful, however the initial renew is then
            // successfully fwd/processed by the leader and as a result
            // the client and leader disagree on where the client is most
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.getOriginalByteBuffer();
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        ReplyHeader hdr =
            new ReplyHeader(request.getMeta().getCxid(), request.getMeta().getZxid(), err.intValue());

        updateStats(request.getMeta());

        try {
            cnxn.sendResponse(hdr, rsp, "response");
            if (request.getMeta().getType() == OpCode.closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG",e);
        }
    }

    void updateStats(Meta meta) {
        zks.serverStats().updateLatency(meta.getCreateTime());
        meta.getCnxn().updateStatsForResponse(meta.getCxid(),
                meta.getZxid(), meta.getType(),
                    meta.getCreateTime(), System.currentTimeMillis());
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

}
