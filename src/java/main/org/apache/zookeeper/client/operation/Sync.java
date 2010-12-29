package org.apache.zookeeper.client.operation;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;

public class Sync extends Operation {
    
    public Sync(String path){
        super(path);
    }

    @Override
    public SyncRequest createRequest() {
        SyncRequest syncRequest = new SyncRequest();
        syncRequest.setPath(path);
        return syncRequest;
    }

    @Override
    public SyncResponse createResponse() {
        return new SyncResponse();
    }

    @Override
    public void receiveResponse(Record response) {

    }

    @Override
    public int getRequestOpCode() {
        return ZooDefs.OpCode.sync;
    }

}