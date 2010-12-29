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
package org.apache.zookeeper.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

/**
 * This class allows us to pass the headers and the relevant records around.
 */
public class Packet {
    public RequestHeader requestHeader;

    public ReplyHeader replyHeader;

    public Record request;

    public Record response;

    /** Client's view of the path (may differ due to chroot) **/
    public String clientPath;
    /** Servers's view of the path (may differ due to chroot) **/
    public String serverPath;

    public boolean finished;

    public AsyncCallback cb;

    public Object ctx;

    public WatchRegistration watchRegistration;
    
    public static final int MAX_LENGTH = Integer.getInteger("jute.maxbuffer",
            4096 * 1024);
    
    public Packet(){};

    public Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
            Record request, Record response, WatchRegistration watchRegistration) {
        this.requestHeader = requestHeader;
        this.replyHeader = replyHeader;
        this.request = request;
        this.response = response;
        this.watchRegistration = watchRegistration;
    }

    public ByteBuffer getAsByteBuffer() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        boa.writeInt(-1, "len"); // We'll fill this in later
        if (requestHeader != null) {
            requestHeader.serialize(boa, "header");
        }
        if (request instanceof ConnectRequest) {
            request.serialize(boa, "connect");
        } else if (request != null) {
            request.serialize(boa, "request");
        }
        baos.close();

        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
        bb.putInt(bb.capacity() - 4);
        bb.rewind();

        return bb;
    }

    public boolean isOrdered() {
        return requestHeader.getType() != OpCode.ping
                && requestHeader.getType() != OpCode.auth;
    }
    
    public static boolean isValidPacketLength(int length){
        return length>=0 && length < MAX_LENGTH;
    }
       
    public synchronized void waitForFinish() throws InterruptedException{
        while (!finished) {
            wait();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("clientPath:" + clientPath);
        sb.append(" serverPath:" + serverPath);
        sb.append(" finished:" + finished);

        sb.append(" header:: " + requestHeader);
        sb.append(" replyHeader:: " + replyHeader);
        sb.append(" request:: " + request);
        sb.append(" response:: " + response);

        // jute toString is horrible, remove unnecessary newlines
        return sb.toString().replaceAll("\r*\n+", " ");
    }
}