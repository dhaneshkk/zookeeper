/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import java.io.IOException;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.Transaction.ProcessTxnResult;

/**
 * Encodes the result of a single part of a multiple operation commit.
 */
public class OpResult {
    private final OpCode type;

    private OpResult(OpCode type) {
        this.type = type;
    }

    /**
     * Encodes the return type as from OpCode.  Can be used
     * to dispatch to the correct cast needed for getting the desired
     * additional result data.
     * @see OpCode
     * @return an integer identifying what kind of operation this result came from.
     */
    public OpCode getType() {
        return type;
    }

    public static OpResult fromProcessTxnResult(OpCode type, ProcessTxnResult txnResult) throws IOException {
        switch (type) {
            case check: return new CheckResult();
            case create: return new CreateResult(txnResult.path);
            case delete: return  new DeleteResult();
            case setData: return new SetDataResult(txnResult.stat);
            case error: return new ErrorResult(txnResult.err) ;
            default: throw new IOException("Invalid type of op");
        }
    }

    /**
     * A result from a create operation.  This kind of result allows the
     * path to be retrieved since the create might have been a sequential
     * create.
     */
    public static class CreateResult extends OpResult {
        private final String path;

        public CreateResult(String path) {
            super(OpCode.create);
            this.path = path;
        }

        public String getPath() {
            return path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            CreateResult other = (CreateResult) o;
            return getType() == other.getType() && path.equals(other.path);
        }

        @Override
        public int hashCode() {
            return getType().hashCode() * 35 + path.hashCode();
        }
    }

    /**
     * A result from a delete operation.  No special values are available.
     */
    public static class DeleteResult extends OpResult {
        public DeleteResult() {
            super(OpCode.delete);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            OpResult opResult = (OpResult) o;
            return getType() == opResult.getType();
        }

        @Override
        public int hashCode() {
            return getType().hashCode();
        }
    }

    /**
     * A result from a setData operation.  This kind of result provides access
     * to the Stat structure from the update.
     */
    public static class SetDataResult extends OpResult {
        private final Stat stat;

        public SetDataResult(Stat stat) {
            super(OpCode.setData);
            this.stat = stat;
        }

        public Stat getStat() {
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            SetDataResult other = (SetDataResult) o;
            return getType() == other.getType() && stat.getMzxid() == other.stat.getMzxid();
        }

        @Override
        public int hashCode() {
            return (int) (getType().hashCode() * 35 + stat.getMzxid());
        }
    }

    /**
     * A result from a version check operation.  No special values are available.
     */
    public static class CheckResult extends OpResult {
        public CheckResult() {
            super(OpCode.check);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            CheckResult other = (CheckResult) o;
            return getType() == other.getType();
        }

        @Override
        public int hashCode() {
            return getType().hashCode();
        }
    }

    /**
     * An error result from any kind of operation.  The point of error results
     * is that they contain an error code which helps understand what happened.
     * @see KeeperException.Code
     *
     */
    public static class ErrorResult extends OpResult {
        private final int err;

        public ErrorResult(int err) {
            super(OpCode.error);
            this.err = err;
        }

        public int getErr() {
            return err;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            ErrorResult other = (ErrorResult) o;
            return getType() == other.getType() && err == other.getErr();
        }

        @Override
        public int hashCode() {
            return getType().hashCode() * 35 + err;
        }
    }
}
