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
