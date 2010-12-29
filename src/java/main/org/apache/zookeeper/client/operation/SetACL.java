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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetACLResponse;

public class SetACL extends Operation {
    private int version;
    private List<ACL> acl;
    private Stat stat;
    
    public SetACL(final String path, List<ACL> acl, int version) throws InvalidACLException{
        super(path);
        this.setAcl(acl);
        this.version = version;
    }
    
    private void setAcl(List<ACL> acl) throws InvalidACLException {
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
        this.acl = acl;
    }

    @Override
    public SetACLRequest createRequest() {
        SetACLRequest setACLRequest = new SetACLRequest();
        
        setACLRequest.setPath(path);
        setACLRequest.setVersion(version);
        setACLRequest.setAcl(acl);
        
        return setACLRequest;
    }

    @Override
    public SetACLResponse createResponse() {
        return new SetACLResponse();
    }

    @Override
    public void receiveResponse(Record response) {
        SetACLResponse setACLResponse = (SetACLResponse)response;
        stat = setACLResponse.getStat();
    }

    @Override
    public int getRequestOpCode() {
        return ZooDefs.OpCode.setACL;
    }

    public Stat getStat(){
        return stat;
    }
}
