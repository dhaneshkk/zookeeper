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

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;

public class SetData extends Operation {
	private int version;
	private byte[] data;
	private Stat stat;
	
	public SetData(String path, byte[] data, int version) {
		super(path);
		this.data = data;
		this.version = version;
	}

	@Override
	public Record createRequest() {		
		SetDataRequest request = new SetDataRequest();
		request.setPath(path);
		request.setData(data);
		request.setVersion(version);
		
		return request;
	}

	@Override
	public Record createResponse() {
		return new SetDataResponse();
	}

	@Override
	public void receiveResponse(Record response) {
		SetDataResponse setDataResponse = (SetDataResponse)response;
		stat = setDataResponse.getStat();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.setData;
	}

	public Stat getStat() {
		return stat;
	}
}
