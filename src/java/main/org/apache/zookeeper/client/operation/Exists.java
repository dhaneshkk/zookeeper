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
	private boolean watching = false;
	private Watcher watcher = null;
	private Stat stat;
	
	public Exists(String path) {
		super(path);
	}
	
	public Exists(String path, boolean watch) {
		this(path);
		this.watching = watch;
	}
	public Exists(String path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
		this.watching = true;
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
			stat = null;
		}
		SetDataResponse existsResponse = (SetDataResponse) response;
		stat = existsResponse.getStat().getCzxid() == -1 ? null: existsResponse.getStat();
	}

	@Override
	public void checkReplyHeader(ReplyHeader header) throws KeeperException {
		if(header.getErr() != 0) {
			if(header.getErr() == KeeperException.Code.NONODE.intValue()) {
				stat = null;
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
		if(watching) {
			return new WatchRegistration.Exists(watcher, path);
		}
		return null;	
	}
}
