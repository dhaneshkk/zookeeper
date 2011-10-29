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

package org.apache.zookeeper.server.util;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.CreateTxnV0;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.txn.MultiTxn;

public class SerializeUtils {
    private static final Map<OpCode, Class<? extends Record>> type2txnRecordClass
        = new EnumMap<OpCode, Class<? extends Record>>(OpCode.class);
    static {
        type2txnRecordClass.put(OpCode.createSession, CreateSessionTxn.class);
        type2txnRecordClass.put(OpCode.create, CreateTxn.class);
        type2txnRecordClass.put(OpCode.delete, DeleteTxn.class);
        type2txnRecordClass.put(OpCode.setData, SetDataTxn.class);
        type2txnRecordClass.put(OpCode.setACL, SetACLTxn.class);
        type2txnRecordClass.put(OpCode.error, ErrorTxn.class);
        type2txnRecordClass.put(OpCode.multi, MultiTxn.class);
        type2txnRecordClass.put(OpCode.check, CheckVersionTxn.class);
    }

    public static Record getRecordForType(OpCode type) throws IOException {
        Class<? extends Record> clazz = type2txnRecordClass.get(type);
        if(clazz == null) throw new IOException("Unsupported Txn with type " + type);

        try { return clazz.newInstance(); }
        catch (InstantiationException e) { throw new RuntimeException(e); }
        catch (IllegalAccessException e) { throw new RuntimeException(e); }
    }

    public static Record deserializeTxn(byte txnBytes[], TxnHeader hdr)
            throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
        InputArchive ia = BinaryInputArchive.getArchive(bais);

        hdr.deserialize(ia, "hdr");
        bais.mark(bais.available());
        if(hdr.getType() == OpCode.closeSession.getInt()) return null;
        final Record txn = getRecordForType(OpCode.fromInt(hdr.getType()));

        try {
            txn.deserialize(ia, "txn");
        } catch(EOFException e) {
            // perhaps this is a V0 Create
            if (hdr.getType() == OpCode.create.getInt()) {
                CreateTxn create = (CreateTxn)txn;
                bais.reset();
                CreateTxnV0 createv0 = new CreateTxnV0();
                createv0.deserialize(ia, "txn");
                // cool now make it V1. a -1 parentCVersion will
                // trigger fixup processing in processTxn
                create.setPath(createv0.getPath());
                create.setData(createv0.getData());
                create.setAcl(createv0.getAcl());
                create.setEphemeral(createv0.getEphemeral());
                create.setParentCVersion(-1);
            } else {
                throw e;
            }
        }
        return txn;
    }

    public static void deserializeSnapshot(DataTree dt,InputArchive ia,
            Map<Long, Integer> sessions) throws IOException {
        int count = ia.readInt("count");
        while (count > 0) {
            long id = ia.readLong("id");
            int to = ia.readInt("timeout");
            sessions.put(id, to);
            count--;
        }
        dt.deserialize(ia, "tree");
    }

    public static void serializeSnapshot(DataTree dt,OutputArchive oa,
            Map<Long, Integer> sessions) throws IOException {
        HashMap<Long, Integer> sessSnap = new HashMap<Long, Integer>(sessions);
        oa.writeInt(sessSnap.size(), "count");
        for (Entry<Long, Integer> entry : sessSnap.entrySet()) {
            oa.writeLong(entry.getKey().longValue(), "id");
            oa.writeInt(entry.getValue().intValue(), "timeout");
        }
        dt.serialize(oa, "tree");
    }


    public static DataNode deserializeNode(InputArchive archive)
            throws IOException {
        archive.startRecord("node");
        byte [] data = archive.readBuffer("data");
        Long acl = archive.readLong("acl");
        StatPersisted stat = new StatPersisted();
        stat.deserialize(archive, "statpersisted");
        archive.endRecord("node");
        return new DataNode(data, acl, stat);
    }

    public static void serializeNode(OutputArchive archive, DataNode node)
            throws IOException {
        // archive.startRecord(node, "node");
        synchronized(node) {
            archive.writeBuffer(node.getData(), "data");
            archive.writeLong(node.getAcl(), "acl");
            node.getStatPersisted().serialize(archive, "statpersisted");
        }
        // archive.endRecord(node, "node");
    }
}
