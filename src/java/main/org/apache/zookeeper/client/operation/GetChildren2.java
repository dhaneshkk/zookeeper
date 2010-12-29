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