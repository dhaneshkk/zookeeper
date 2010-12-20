package org.apache.zookeeper.client.operation;

import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.WatchRegistration;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.common.Path;

public class GetChildren extends Operation {
	private Watcher watcher = null;
	private boolean watching = false;
	private List<String> children;
	
	public GetChildren(Path path) {
		super(path);
	}
	
	public GetChildren(Path path, boolean watch) {
		this(path);
		this.watching = watch;
	}
	
	public GetChildren(Path path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
		this.watching = true;
	}
	
	@Override
	public Record createRequest(ChrootPathTranslator chroot) {
		final String serverPath = chroot.toServer(path).toString();
	
		GetChildrenRequest request = new GetChildrenRequest();
		request.setPath(serverPath);
		request.setWatch(watcher != null);
		
		return request;
	}

	@Override
	public Record createResponse() {
		return new GetChildrenResponse();
	}

	@Override
	public void receiveResponse(ChrootPathTranslator chroot, Record response) {
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
	
	// Return a ChildrenWatchRegistration object, if there is a order for watching
	@Override
	public WatchRegistration.Child getWatchRegistration(String serverPath) {
		if(watching) {
			return new WatchRegistration.Child(watcher, serverPath);
		}
		return null;	
	}
}
