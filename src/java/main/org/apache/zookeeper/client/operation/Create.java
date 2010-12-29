package org.apache.zookeeper.client.operation;

import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;

public class Create extends Operation {
	private byte data[];
	private List<ACL> acl;
	private CreateMode createMode;
	private String responsePath;
	
	public Create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws InvalidACLException {
		super(path);
		this.data = data;	
		this.createMode = createMode;
		this.responsePath = null;
		this.setAcl(acl);
	}
	
	public byte[] getData() {
		return data;
	}
		
	public List<ACL> getAcl() {
		return acl;
	}
	
	public void setAcl(List<ACL> acl) throws InvalidACLException {
		if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
		this.acl = acl;
	}
	
	public CreateMode getCreateMode() {
		return createMode;
	}
	
	public String getResponsePath() {
		return responsePath;
	}
	
	@Override
	public Record createRequest() {
		CreateRequest request = new CreateRequest();
		
		request.setData(data);
		request.setFlags(createMode.toFlag());
		request.setPath(this.path);
		request.setAcl(acl);
		
		return request;
	}

	@Override
	public void receiveResponse(Record response) {	
		CreateResponse createResponse = (CreateResponse) response;
		this.responsePath = createResponse.getPath();
	}

	@Override
	public int getRequestOpCode() {
		return ZooDefs.OpCode.create;
	}

	@Override
	public Record createResponse() {
		return new CreateResponse();
	}
}