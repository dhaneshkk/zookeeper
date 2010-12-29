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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.client.Executor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

public class RetryUntilConnected {
    Executor executor;

	public RetryUntilConnected(Executor executor) {
		super();
		this.executor = executor;
	}
    
	public void execute(Operation op) throws InterruptedException, KeeperException{
        while (true) {
            try {
                executor.execute(op);
                return;
            } catch (ConnectionLossException e) {
                // we give the event thread some time to update the status to 'Disconnected'
                Thread.yield();
                waitUntilConnected();
            } catch (SessionExpiredException e) {
                // we give the event thread some time to update the status to 'Expired'
                Thread.yield();
                waitUntilConnected();
            }
        }

	}
	
    private void waitUntilConnected() {
        waitUntilConnected(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private boolean waitUntilConnected(long time, TimeUnit timeUnit) {
    	Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
    	while(true){
    		if( executor.getClientCnxn().getState() == ZooKeeper.States.CONNECTED){
    			return true;
    		}
    		try {
				wait(300);
			} catch (InterruptedException e) {
				// ignore
			}
    		if( new Date().after(timeout) ){
    			return false;
    		}
    	}

    }
}
