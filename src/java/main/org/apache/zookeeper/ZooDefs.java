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

package org.apache.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;

public class ZooDefs {
    public enum OpCode {
        notification(0, "", "notification", null),
        create(1, "CREA", "create", CreateRequest.class),
        delete(2, "DELE", "delete", DeleteRequest.class),
        exists(3, "EXIS", "exists", ExistsRequest.class),
        getData(4, "GETD", "getData", GetDataRequest.class),
        setData(5, "SETD", "setData", SetDataRequest.class),
        getACL(6, "GETA", "getACL", GetACLRequest.class),
        setACL(7, "SETA", "setACL", SetACLRequest.class),
        getChildren(8, "GETC", "getChildren", GetChildrenRequest.class),
        sync(9, "SYNC", "sync", SyncRequest.class),
        ping(11, "PING", "ping", null),
        getChildren2(12, "GETC", "getChildren2", GetChildren2Request.class),
        check(13, "CHEC", "check", CheckVersionRequest.class),
        multi(14, "MULT", "multi", MultiTransactionRecord.class),
        auth(100, "", "", null),
        setWatches(101, "SETW", "setWatches", SetWatches.class),
        sasl(102, "", "", null),
        createSession(-10, "SESS", "createSession", null),
        closeSession(-11, "CLOS", "closeSession", null),
        error(-1, "", "error", null);

        public final String string;
        public final String longString;
        private final int code;
        public final Class<? extends Record> recordClass;


        private final static Set<OpCode> writeOps = EnumSet.of(create, delete, setData, setACL, createSession, closeSession, multi, check);
        private final static Set<OpCode> readOnlyOps = EnumSet.complementOf(EnumSet.of(sync, create, delete, setData, setACL));

        private static final Map<Integer, OpCode> codesToEnums;

        static {
            codesToEnums = new HashMap<Integer, OpCode>(values().length);
            for(OpCode opCode : values()) {
                codesToEnums.put(opCode.code, opCode);
            }
        }

        OpCode(int code, String string, String longString, Class<? extends Record> recordClass) {
            this.code = code;
            this.string = string;
            this.longString = longString;
            this.recordClass = recordClass;
        }

        public static OpCode fromInt(int code) {
            OpCode opCode = codesToEnums.get(code);
            if(opCode==null) throw new IllegalArgumentException("No OpCode for code "+code);
            return opCode;
        }

        public int getInt() {
            return code;
        }

        public boolean is(int code) {
            return this.code == code;
        }

        public boolean isNot(int code) {
            return !is(code);
        }

        public boolean isWriteOp() {
            return writeOps.contains(this);
        }

        public boolean isReadOnlyOp() {
            return readOnlyOps.contains(this);
        }
    }

    public interface Perms {
        int READ = 1 << 0;

        int WRITE = 1 << 1;

        int CREATE = 1 << 2;

        int DELETE = 1 << 3;

        int ADMIN = 1 << 4;

        int ALL = READ | WRITE | CREATE | DELETE | ADMIN;
    }

    public interface Ids {
        /**
         * This Id represents anyone.
         */
        public final Id ANYONE_ID_UNSAFE = new Id("world", "anyone");

        /**
         * This Id is only usable to set ACLs. It will get substituted with the
         * Id's the client authenticated with.
         */
        public final Id AUTH_IDS = new Id("auth", "");

        /**
         * This is a completely open ACL .
         */
        public final ArrayList<ACL> OPEN_ACL_UNSAFE = new ArrayList<ACL>(
                Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID_UNSAFE)));

        /**
         * This ACL gives the creators authentication id's all permissions.
         */
        public final ArrayList<ACL> CREATOR_ALL_ACL = new ArrayList<ACL>(
                Collections.singletonList(new ACL(Perms.ALL, AUTH_IDS)));

        /**
         * This ACL gives the world the ability to read.
         */
        public final ArrayList<ACL> READ_ACL_UNSAFE = new ArrayList<ACL>(
                Collections
                        .singletonList(new ACL(Perms.READ, ANYONE_ID_UNSAFE)));
    }
}
