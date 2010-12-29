package org.apache.zookeeper.client;

import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.client.operation.Operation;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

public class Executor {
 	
	private ClientCnxn clientCnxn;
	
	public Executor(ClientCnxn clientCnxn) {
		this.clientCnxn = clientCnxn;		
	}
	
	public ClientCnxn getClientCnxn() {
		return clientCnxn;
	}
	
	/**
	 * Execute the operation synchronously.
	 */
	public void execute(Operation op) throws InterruptedException, KeeperException {
		String path = op.getPath();
		Record response = op.createResponse();
    	Record request = op.createRequest();
		RequestHeader header = new RequestHeader();
		WatchRegistration watchRegistration = op.getWatchRegistration();

		header.setType(op.getRequestOpCode());
		ReplyHeader reply = clientCnxn.submitRequest(header, request , response, watchRegistration);
		op.checkReplyHeader(reply);
		op.receiveResponse(response);		
	}

	/**
	 * Send the operation for asynchronous execution.
	 */
	public void send(Operation op, AsyncCallback cb, Object context) {
		Record response = op.createResponse();
		Record request = op.createRequest();
		RequestHeader header = new RequestHeader();
		ReplyHeader reply = new ReplyHeader();
		String path = op.getPath();
		WatchRegistration watchRegistration = op.getWatchRegistration();
	
		header.setType(op.getRequestOpCode()); 
		clientCnxn.queuePacket(header, reply, request, response, cb, path, context, watchRegistration);
	}
}
