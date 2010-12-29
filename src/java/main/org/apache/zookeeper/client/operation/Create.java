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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;

public class Create extends Operation {
	private byte data[];
	private List<ACL> acl;
	private CreateMode createMode;
	private String responsePath;
	
	public Create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws InvalidACLException {
		super(path, createMode.isSequential());
		this.data = data;	
		this.createMode = createMode;
		this.setAcl(acl);
	}
	
	public byte[] getData() {
		return data;
	}
		
	public List<ACL> getAcl() {
		return acl;
	}
	
	private void setAcl(List<ACL> acl) throws InvalidACLException {
		if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
		this.acl = acl;
	}
	
	public CreateMode getCreateMode() {
		return createMode;
	}
	
	public String getResponsePath() {
		return responsePath;
	}
	
	@Override
	public CreateRequest createRequest() {
		CreateRequest request = new CreateRequest();
		
		request.setData(data);
		request.setFlags(createMode.toFlag());
		request.setPath(this.path);
		request.setAcl(acl);
		
		return request;
	}

	@Override
	public void receiveResponse(Record response) {	
		CreateResponse createResponse = (CreateResponse) response;
		this.responsePath = createResponse.getPath();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.create;
	}

	@Override
	public CreateResponse createResponse() {
		return new CreateResponse();
	}
}
