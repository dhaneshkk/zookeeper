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

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.MultiHeader;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.ErrorResponse;
import org.apache.zookeeper.server.Transaction.ProcessTxnResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Handles the response from a multi request.  Such a response consists of
 * a sequence of responses each prefixed by a MultiResponse that indicates
 * the type of the response.  The end of the list is indicated by a MultiHeader
 * with a negative type.  Each individual response is in the same format as
 * with the corresponding operation in the original request list.
 */
public class MultiResponse implements Record, Iterable<OpResult> {
    private final List<OpResult> results;

    public MultiResponse() {
        this.results = new ArrayList<OpResult>();
    }

    public MultiResponse(List<ProcessTxnResult> results) throws IOException {
        this.results = new ArrayList<OpResult>(results.size());
        for (ProcessTxnResult txnResult : results) {
            this.results.add(OpResult.fromProcessTxnResult(txnResult.type, txnResult));
        }
    }

    // this method is only used in tests
    public static MultiResponse fromOpResultsList(List<OpResult> results) {
        MultiResponse multiResponse = new MultiResponse();
        for(OpResult result : results) {
            multiResponse.results.add(result);
        }
        return multiResponse;
    }

    @Override
    public Iterator<OpResult> iterator() {
        return results.iterator();
    }

    public int size() {
        return results.size();
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);

        for (OpResult result : results) {
            OpCode opCode = result.getType();
            int err = opCode == OpCode.error ? ((OpResult.ErrorResult)result).getErr() : 0;

            new MultiHeader(result.getType().getInt(), false, err).serialize(archive, tag);

            switch (opCode) {
                case create:
                    new CreateResponse(((OpResult.CreateResult) result).getPath()).serialize(archive, tag);
                    break;
                case delete:
                case check:
                    break;
                case setData:
                    new SetDataResponse(((OpResult.SetDataResult) result).getStat()).serialize(archive, tag);
                    break;
                case error:
                    new ErrorResponse(((OpResult.ErrorResult) result).getErr()).serialize(archive, tag);
                    break;
                default:
                    throw new IOException("Invalid type " + result.getType() + " in MultiResponse");
            }
        }
        new MultiHeader(-1, true, -1).serialize(archive, tag);
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        MultiHeader h = new MultiHeader();
        h.deserialize(archive, tag);
        while (!h.getDone()) {
            switch (OpCode.fromInt(h.getType())) {
                case create:
                    CreateResponse cr = new CreateResponse();
                    cr.deserialize(archive, tag);
                    results.add(new OpResult.CreateResult(cr.getPath()));
                    break;

                case delete:
                    results.add(new OpResult.DeleteResult());
                    break;

                case setData:
                    SetDataResponse sdr = new SetDataResponse();
                    sdr.deserialize(archive, tag);
                    results.add(new OpResult.SetDataResult(sdr.getStat()));
                    break;

                case check:
                    results.add(new OpResult.CheckResult());
                    break;

                case error:
                    //FIXME: need way to more cleanly serialize/deserialize exceptions
                    ErrorResponse er = new ErrorResponse();
                    er.deserialize(archive, tag);
                    results.add(new OpResult.ErrorResult(er.getErr()));
                    break;

                default:
                    throw new IOException("Invalid type " + h.getType() + " in MultiResponse");
            }
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }

    public List<OpResult> getResultList() {
        return results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MultiResponse)) return false;

        MultiResponse other = (MultiResponse) o;

        if (results != null) {
            Iterator<OpResult> i = other.results.iterator();
            for (OpResult result : results) {
                if (i.hasNext()) {
                    if (!result.equals(i.next())) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return !i.hasNext();
        }
        else return other.results == null;
    }

    @Override
    public int hashCode() {
        int hash = results.size();
        for (OpResult result : results) {
            hash = (hash * 35) + result.hashCode();
        }
        return hash;
    }
}
