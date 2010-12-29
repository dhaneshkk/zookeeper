package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.DeleteRequest;

public class Delete extends Operation {
	private int version;
	
	public Delete(String path, int version) {
		super(path);
		this.version = version;
	}

	public int getVersion() {
		return version;
	}
	
	@Override
	public Record createRequest() {
		DeleteRequest request = new DeleteRequest();
		request.setPath(path);
		request.setVersion(version);
		
		return request;
	}

	@Override
	public void receiveResponse(Record response) {
		// Nothing to do	
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.delete;
	}

	@Override
	public Record createResponse() {
		return null;
	}
}
