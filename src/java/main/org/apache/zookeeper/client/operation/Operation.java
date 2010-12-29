package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.client.WatchRegistration;
import org.apache.zookeeper.proto.ReplyHeader;

public abstract class Operation {
	protected String path;
	
	protected Operation(String path) {
		this.path = path;
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