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
import org.apache.zookeeper.client.WatchRegistration;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.proto.ReplyHeader;

public abstract class Operation {
	protected String path;
	protected Watcher watcher = null;
	
	protected Operation(String path, boolean isSequential) {
	    PathUtils.validatePath(path, isSequential);
		this.path = path;
	}
	
	protected Operation(String path) {
	    this(path, false);
	}
	
	public String getPath() {
		return path;
	}
	
	public abstract Record createRequest();
	
	public abstract Record createResponse();
  
	public abstract void receiveResponse(Record response);
  
	public abstract int getRequestOpCode();

    public WatchRegistration getWatchRegistration() {
        return null;
    }

	public void checkReplyHeader(ReplyHeader header) throws KeeperException {
		if(header.getErr() != 0) {
			throw KeeperException.create(KeeperException.Code.get(header.getErr()), path.toString());
		}
	}
}
