package org.apache.zookeeper.client.operation;

import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.client.WatchRegistration;
import org.apache.zookeeper.data.Stat;

public class GetChildren extends Operation {
	private Watcher watcher = null;
	private List<String> children;
	private Stat stat;
	
	public GetChildren(String path) {
		super(path);
	}
	
	public GetChildren(String path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
	}
	
	@Override
	public GetChildrenRequest createRequest() {
		GetChildrenRequest request = new GetChildrenRequest();
		request.setPath(path);
		request.setWatch(watcher != null);
		
		return request;
	}

	@Override
	public GetChildrenResponse createResponse() {
		return new GetChildrenResponse();
	}

	@Override
	public void receiveResponse(Record response) {
		GetChildrenResponse getChildrenResponse = (GetChildrenResponse)response;
		this.children = getChildrenResponse.getChildren();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.getChildren;
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