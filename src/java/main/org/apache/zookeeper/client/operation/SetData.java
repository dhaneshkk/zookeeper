package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;

public class SetData extends Operation {
	private int version;
	private byte[] data;
	private Stat stat;
	
	public SetData(String path, byte[] data, int version) {
		super(path);
		this.data = data;
		this.version = version;
	}

	@Override
	public Record createRequest() {		
		SetDataRequest request = new SetDataRequest();
		request.setPath(path);
		request.setData(data);
		request.setVersion(version);
		
		return request;
	}

	@Override
	public Record createResponse() {
		return new SetDataResponse();
	}

	@Override
	public void receiveResponse(Record response) {
		SetDataResponse setDataResponse = (SetDataResponse)response;
		stat = setDataResponse.getStat();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.setData;
	}

	public Stat getStat() {
		return stat;
	}
}