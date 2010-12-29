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
	    Packet packet = createPacket(op);
		clientCnxn.submitRequest(packet);
		op.checkReplyHeader(packet.replyHeader);
		op.receiveResponse(packet.response);		
	}

	/**
	 * Send the operation for asynchronous execution.
	 */
	public void send(Operation op, AsyncCallback cb, Object context) {
	    Packet packet = createPacket(op);
        packet.cb = cb;
        packet.ctx = context;
		clientCnxn.queuePacket(packet);
	}
	
	private Packet createPacket(Operation op){
	    Record response = op.createResponse();
	    Record request = op.createRequest();
	    RequestHeader requestHeader = new RequestHeader();
	    requestHeader.setType(op.getRequestOpCode()); 
	    ReplyHeader replyHeader = new ReplyHeader();
	    String clientPath = op.getPath();
	    String serverPath = prependChroot(clientPath);
	    WatchRegistration watchRegistration = op.getWatchRegistration();
	    
	    Packet packet = new Packet(requestHeader, replyHeader, request, response, watchRegistration);
        packet.clientPath = clientPath;
        packet.serverPath = serverPath;
        
        return packet;
	}
	
    /**
     * Prepend the chroot to the client path (if present). The expectation of
     * this function is that the client path has been validated before this
     * function is called
     * @param clientPath path to the node
     * @return server view of the path (chroot prepended to client path)
     */
    private String prependChroot(String clientPath) {
        String chrootPath = clientCnxn.getChrootPath();
        if (chrootPath != null) {
            // handle clientPath = "/"
            if (clientPath.length() == 1) {
                return chrootPath;
            }
            return chrootPath + clientPath;
        } else {
            return clientPath;
        }
    }
}
