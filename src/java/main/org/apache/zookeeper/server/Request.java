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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.AccessControlList.Identifier;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This is the structure that represents a request moving through a chain of
 * RequestProcessors. There are various pieces of information that is tacked
 * onto the request as it is processed.
 */
public final class Request {
    public final static Request requestOfDeath = new Request(Meta.EMPTY, null);

    private final Meta meta;

    private final ByteBuffer originalByteBuffer;

    private final TxnHeader hdr;

    private final Record txn;

    private KeeperException e;

    public Request(Meta meta, ByteBuffer originalByteBuffer, TxnHeader hdr, Record txn) {
        if(meta == null) throw new IllegalArgumentException("null given to Request constructor.");
        this.meta = meta;
        this.originalByteBuffer = originalByteBuffer;
        this.hdr = hdr;
        this.txn = txn;
    }

    public Request(Meta meta, ByteBuffer originalByteBuffer) {
        this(meta, originalByteBuffer, null, null);
    }

    public TxnHeader getHdr() { return hdr; }
    public Record getTxn() { return txn; }
    public ByteBuffer getOriginalByteBuffer() { return originalByteBuffer; }
    public Meta getMeta() { return meta; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("sessionid:0x").append(Long.toHexString(meta.getSessionId()))
            .append(" type:").append(meta.getType().longString)
            .append(" cxid:0x").append(Long.toHexString(meta.getCxid()))
            .append(" zxid:0x").append(Long.toHexString(hdr == null ? -2 : hdr.getZxid()))
            .append(" txntype:").append(hdr == null ? "unknown" : "" + hdr.getType());

        // best effort to print the path assoc with this request
        String path = "n/a";
        if (meta.getType() != OpCode.createSession
                && meta.getType() != OpCode.setWatches
                && meta.getType() != OpCode.closeSession
                && originalByteBuffer != null
                && originalByteBuffer.remaining() >= 4)
        {
            try {
                // make sure we don't mess with request itself
                ByteBuffer rbuf = originalByteBuffer.asReadOnlyBuffer();
                rbuf.clear();
                int pathLen = rbuf.getInt();
                // sanity check
                if (pathLen >= 0
                        && pathLen < 4096
                        && rbuf.remaining() >= pathLen)
                {
                    byte b[] = new byte[pathLen];
                    rbuf.get(b);
                    path = new String(b);
                }
            } catch (Exception e) {
                // ignore - can't find the path, will output "n/a" instead
            }
        }
        sb.append(" reqpath:").append(path);

        return sb.toString();
    }

    public void setException(KeeperException e) {
        this.e = e;
    }

    public KeeperException getException() {
        return e;
    }

    public Record deserializeRequestRecord() throws IOException {
        Record requestRecord;
        try {
            requestRecord = meta.getType().recordClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        ByteBufferInputStream.byteBuffer2Record(originalByteBuffer, requestRecord);
        return requestRecord;
    }

    public static final class Meta {
        public static final Meta EMPTY = new Meta(0l, 0, -1l, null);
        private final long sessionId;
        private final int cxid;
        private final long zxid;
        private final OpCode type;
        private final ServerCnxn cnxn;
        private final List<Identifier> authInfo;
        private final Object owner;
        private final long createTime;

        public Meta(long sessionId, int cxid, long zxid, OpCode type) {
            this(sessionId, cxid, zxid, type, null, null, null);
        }

        public Meta(long sessionId, int cxid, long zxid, OpCode type, ServerCnxn cnxn, List<Identifier> authInfo, Object owner) {
            this(sessionId, cxid, zxid, type, cnxn, authInfo, owner, System.currentTimeMillis());
        }

        public Meta(long sessionId, int cxid, long zxid, OpCode type, ServerCnxn cnxn, List<Identifier> authInfo, Object owner, long createTime) {
            this.sessionId = sessionId;
            this.cxid = cxid;
            this.zxid = zxid;
            this.type = type;
            this.cnxn = cnxn;
            this.authInfo = authInfo;
            this.owner = owner;
            this.createTime = createTime;
        }

        public Meta cloneWithError() {
            return new Meta(sessionId, cxid, zxid, OpCode.error, cnxn, authInfo, owner, createTime);
        }

        public Meta cloneWithZxid(long newZxid) {
            return new Meta(sessionId, cxid, newZxid, type, cnxn, authInfo, owner, createTime);
        }

        public long getSessionId() { return sessionId; }
        public int getCxid() { return cxid; }
        public long getZxid() { return zxid; }
        public OpCode getType() { return type; }
        public ServerCnxn getCnxn() { return cnxn; }
        public List<Identifier> getAuthInfo() { return authInfo; }
        public Object getOwner() { return owner; }
        public long getCreateTime() { return createTime; }
    }
}
