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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.client.WatchRegistration;

public class Exists extends Operation {
	private Stat stat;
	
	public Exists(String path) {
		super(path);
	}
	
	public Exists(String path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
	}
    
	public Stat getStat() {
		return stat;
	}

	@Override
	public Record createRequest() {
		ExistsRequest request = new ExistsRequest();
		
		request.setPath(path);
		request.setWatch(watcher != null);
		
		return request;		
	}

	@Override
	public Record createResponse() {
		return new SetDataResponse();
	}

	@Override
	public void receiveResponse(Record response) {
		if(response == null)
		{
			return;
		}
		SetDataResponse existsResponse = (SetDataResponse) response;
		stat = existsResponse.getStat().getCzxid() == -1 ? null: existsResponse.getStat();
	}

	@Override
	public void checkReplyHeader(ReplyHeader header) throws KeeperException {
		if(header.getErr() != 0) {
			if(header.getErr() == KeeperException.Code.NONODE.intValue()) {
				stat = null;
				return;
			}
			throw KeeperException.create(KeeperException.Code.get(header.getErr()), path.toString()); 
		}	
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.exists;
	}
	
	// Return a ExistsWatchRegistration object, if there is a order for watching
	@Override
	public WatchRegistration.Exists getWatchRegistration() {
	    if(watcher != null) {
	        return new WatchRegistration.Exists(watcher, path);
	    }
	    return null;	
	}
}
