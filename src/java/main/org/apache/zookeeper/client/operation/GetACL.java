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
package org.apache.zookeeper.client.operation;

import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;

public class GetACL extends Operation {
    private Stat stat;
    private List<ACL> acl;
    
    public GetACL(final String path){
        super(path);
    }
    
    @Override
    public GetACLRequest createRequest() {
        GetACLRequest request = new GetACLRequest();
        request.setPath(path);
        return request;
    }

    @Override
    public GetACLResponse createResponse() {
        return new GetACLResponse();
    }

    @Override
    public void receiveResponse(Record response) {
        GetACLResponse getACLResponse = (GetACLResponse)response;
        stat = getACLResponse.getStat();
        acl = getACLResponse.getAcl();
    }

    @Override
    public int getRequestOpCode() {
        return ZooDefs.OpCode.getACL;
    }

    public Stat getStat(){
        return stat;
    }
    
    public List<ACL> getAcl(){
        return acl;
    }
}
