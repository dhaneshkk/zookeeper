package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.client.WatchRegistration;

public class GetData extends Operation {
	private Watcher watcher = null;
	private byte[] data;
	private Stat stat;
	
	public GetData(String path) {
		super(path);
	}
	
	public GetData(String path, Watcher watcher) {
		this(path);
		this.watcher = watcher;
	}

	@Override
	public Record createRequest() {		
		GetDataRequest request = new GetDataRequest();
		request.setPath(path);
		request.setWatch(watcher != null);
		
		return request;
	}

	@Override
	public Record createResponse() {
		return new GetDataResponse();
	}

	@Override
	public void receiveResponse(Record response) {
		GetDataResponse getDataResponse = (GetDataResponse)response;
		
		data = getDataResponse.getData();
		stat = getDataResponse.getStat();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.getData;
	}

	public byte[] getData() {
		return data;
	}
	
	@Override
	public WatchRegistration.Data getWatchRegistration() {
		if(watcher != null) {
			return new WatchRegistration.Data(watcher, path);
		}
		return null;	
	}

	public Stat getStat() {
		return stat;
	}
}