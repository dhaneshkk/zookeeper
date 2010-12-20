package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.common.Path;

public class Delete extends Operation {
	private int version;
	
	public Delete(Path path, int version) {
		super(path);
		this.version = version;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
	
	@Override
	public Record createRequest(ChrootPathTranslator chroot) {
		final String serverPath = chroot.toServer(path).toString();

		DeleteRequest request = new DeleteRequest();
		request.setPath(serverPath);
		request.setVersion(version);
		
		return request;
	}

	@Override
	public void receiveResponse(ChrootPathTranslator chroot, Record response) {
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
