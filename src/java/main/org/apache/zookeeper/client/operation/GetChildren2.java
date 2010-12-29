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
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.client.WatchRegistration;
import org.apache.zookeeper.data.Stat;

public class GetChildren2 extends Operation {
	private Watcher watcher = null;
	private List<String> children;
	private Stat stat;
	
	public GetChildren2(String path) {
		super(path);
	}
	
	public GetChildren2(String path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
	}
	
	@Override
	public GetChildren2Request createRequest() {
		GetChildren2Request request = new GetChildren2Request();
		request.setPath(path);
		request.setWatch(watcher != null);
		
		return request;
	}

	@Override
	public GetChildren2Response createResponse() {
		return new GetChildren2Response();
	}

	@Override
	public void receiveResponse(Record response) {
		GetChildren2Response getChildren2Response = (GetChildren2Response)response;
		this.children = getChildren2Response.getChildren();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.getChildren2;
	}

	public List<String> getChildren() {
		return children;
	}
	
	@Override
	public WatchRegistration.Child getWatchRegistration() {
		if(watcher != null) {
			return new WatchRegistration.Child(watcher, path);
		}
		return null;
	}

    public Stat getStat() {
        return stat;
    }
}
