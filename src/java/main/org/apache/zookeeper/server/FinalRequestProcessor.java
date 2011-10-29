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
import java.util.List;

import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateResponse;
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
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.ErrorTxn;

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

        ProcessTxnResult rc = null;
        synchronized (zks.outstandingChanges) {
            while (!zks.outstandingChanges.isEmpty()
                    && zks.outstandingChanges.get(0).zxid <= request.zxid) {
                ChangeRecord cr = zks.outstandingChanges.remove(0);
                if (cr.zxid < request.zxid) {
                    LOG.warn("Zxid outstanding "
                            + cr.zxid
                            + " is less than current " + request.zxid);
                }
                if (zks.outstandingChangesForPath.get(cr.path) == cr) {
                    zks.outstandingChangesForPath.remove(cr.path);
                }
            }
            if (request.getHdr() != null) {
                rc = zks.getZKDatabase().processTxn(request.getHdr(), request.getTxn());
                if (request.type == OpCode.createSession) {
                    if (request.getTxn() instanceof CreateSessionTxn) {
                        CreateSessionTxn cst = (CreateSessionTxn) request.getTxn();
                        zks.sessionTracker.addSession(request.sessionId, cst
                                .getTimeOut());
                    } else {
                        LOG.warn("*****>>>>> Got "
                                + request.getTxn().getClass() + " "
                                + request.getTxn().toString());
                    }
                } else if (request.type == OpCode.closeSession) {
                    zks.sessionTracker.removeSession(request.sessionId);
                }
                zks.getZKDatabase().addCommittedProposal(request);
            }
        }

        if (request.getHdr() != null && OpCode.closeSession.is(request.getHdr().getType())) {
            ServerCnxnFactory scxn = zks.getServerCnxnFactory();
            // this might be possible since
            // we might just be playing diffs from the leader
            if (scxn != null && request.cnxn == null) {
                // calling this if we have the cnxn results in the client's
                // close session response being lost - we've already closed
                // the session/socket here before we can send the closeSession
                // in the switch block below
                scxn.closeSession(request.sessionId);
                return;
            }
        }

        if (request.cnxn == null) {
            return;
        }
        ServerCnxn cnxn = request.cnxn;

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
            switch (request.type) {
            case ping: {
                updateStats(request);
                cnxn.sendResponse(new ReplyHeader(-2,
                        zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null, "response");
                return;
            }
            case createSession: {
                updateStats(request);
                zks.finishSessionInit(request.cnxn, true);
                return;
            }
            case multi: {
                rsp = new MultiResponse(rc.multiResult);
                break;
            }
            case create: {
                rsp = new CreateResponse(rc.path);
                err = Code.get(rc.err);
                break;
            }
            case delete: {
                err = Code.get(rc.err);
                break;
            }
            case setData: {
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case setACL: {
                rsp = new SetACLResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case closeSession: {
                err = Code.get(rc.err);
                break;
            }
            case sync: {
                SyncRequest syncRequest = (SyncRequest)request.deserializeRequestRecord();
                rsp = new SyncResponse(syncRequest.getPath());
                break;
            }
            case check: {
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case exists: {
                // TODO we need to figure out the security requirement for this!
                ExistsRequest existsRequest = (ExistsRequest)request.deserializeRequestRecord();
                String path = existsRequest.getPath();
                if (path.indexOf('\0') != -1) {
                    throw new KeeperException.BadArgumentsException();
                }
                Stat stat = zks.getZKDatabase().statNode(path, existsRequest
                        .getWatch() ? cnxn : null);
                rsp = new ExistsResponse(stat);
                break;
            }
            case getData: {
                GetDataRequest getDataRequest = (GetDataRequest)request.deserializeRequestRecord();
                DataNode n = zks.getZKDatabase().getNode(getDataRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                Long aclL;
                synchronized(n) {
                    aclL = n.acl;
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().convertLong(aclL),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                Stat stat = new Stat();
                byte b[] = zks.getZKDatabase().getData(getDataRequest.getPath(), stat,
                        getDataRequest.getWatch() ? cnxn : null);
                rsp = new GetDataResponse(b, stat);
                break;
            }
            case setWatches: {
                // XXX We really should NOT need this!!!!
                request.request.rewind();
                SetWatches setWatches = (SetWatches)request.deserializeRequestRecord();
                long relativeZxid = setWatches.getRelativeZxid();
                zks.getZKDatabase().setWatches(relativeZxid,
                        setWatches.getDataWatches(),
                        setWatches.getExistWatches(),
                        setWatches.getChildWatches(), cnxn);
                break;
            }
            case getACL: {
                GetACLRequest getACLRequest = (GetACLRequest)request.deserializeRequestRecord();
                Stat stat = new Stat();
                List<ACL> acl =
                    zks.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                rsp = new GetACLResponse(acl, stat);
                break;
            }
            case getChildren: {
                GetChildrenRequest getChildrenRequest = (GetChildrenRequest)request.deserializeRequestRecord();
                DataNode n = zks.getZKDatabase().getNode(getChildrenRequest.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                Long aclG;
                synchronized(n) {
                    aclG = n.acl;

                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().convertLong(aclG),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = zks.getZKDatabase().getChildren(
                        getChildrenRequest.getPath(), null, getChildrenRequest
                                .getWatch() ? cnxn : null);
                rsp = new GetChildrenResponse(children);
                break;
            }
            case getChildren2: {
                GetChildren2Request getChildren2Request = (GetChildren2Request)request.deserializeRequestRecord();
                Stat stat = new Stat();
                DataNode n = zks.getZKDatabase().getNode(getChildren2Request.getPath());
                if (n == null) {
                    throw new KeeperException.NoNodeException();
                }
                Long aclG;
                synchronized(n) {
                    aclG = n.acl;
                }
                PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().convertLong(aclG),
                        ZooDefs.Perms.READ,
                        request.authInfo);
                List<String> children = zks.getZKDatabase().getChildren(
                        getChildren2Request.getPath(), stat, getChildren2Request
                                .getWatch() ? cnxn : null);
                rsp = new GetChildren2Response(children, stat);
                break;
            }
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
            ByteBuffer bb = request.request;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }

        ReplyHeader hdr =
            new ReplyHeader(request.cxid, request.zxid, err.intValue());

        updateStats(request);

        try {
            cnxn.sendResponse(hdr, rsp, "response");
            if (request.type == OpCode.closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG",e);
        }
    }

    private void updateStats(Request request) {
        zks.serverStats().updateLatency(request.createTime);
        request.cnxn.updateStatsForResponse(request.cxid, request.zxid, request.type,
                    request.createTime, System.currentTimeMillis());
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }

}
