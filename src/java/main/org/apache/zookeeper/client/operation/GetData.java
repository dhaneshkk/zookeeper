package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.client.WatchRegistration;
import org.apache.zookeeper.common.Path;

public class GetData extends Operation {
	private Path path;
	private boolean watching = false;
	private Watcher watcher = null;
	private byte[] data;
	private Stat stat;
	
	public GetData(Path path) {
		super(path);
	}
	
	public GetData(Path path, boolean watch) {
		this(path);
		this.watching = watch;
	}
	
	public GetData(Path path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
		this.watching = true;
	}

	@Override
	public Record createRequest(ChrootPathTranslator chroot) {
		final String serverPath = chroot.toServer(path).toString();
		
		GetDataRequest request = new GetDataRequest();
		request.setPath(serverPath);
		request.setWatch(watcher != null);
		
		return request;
	}

	@Override
	public Record createResponse() {
		return new GetDataResponse();
	}

	@Override
	public void receiveResponse(ChrootPathTranslator chroot, Record response) {
		GetDataResponse getDataResponse = (GetDataResponse)response;
		
		this.data = getDataResponse.getData();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.getData;
	}

	public byte[] getData() {
		return data;
	}
	
	// Return a DataWatchRegistration object, if there is a order for watching
	@Override
	public WatchRegistration.Data getWatchRegistration(String serverPath) {
		if(watching) {
			return new WatchRegistration.Data(watcher, serverPath);
		}
		return null;	
	}

	public Stat getStat() {
		return stat;
	}
}
