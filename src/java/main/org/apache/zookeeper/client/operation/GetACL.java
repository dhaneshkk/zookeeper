package org.apache.zookeeper.client.operation;

import java.util.List;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;

public class GetACL extends Operation {
    private Stat stat;
    private List<ACL> acl;
    
    public GetACL(final String path){
        super(path);
    }
    
    @Override
    public GetACLRequest createRequest() {
        GetACLRequest request = new GetACLRequest();
        request.setPath(path);
        return request;
    }

    @Override
    public GetACLResponse createResponse() {
        return new GetACLResponse();
    }

    @Override
    public void receiveResponse(Record response) {
        GetACLResponse getACLResponse = (GetACLResponse)response;
        stat = getACLResponse.getStat();
        acl = getACLResponse.getAcl();
    }

    @Override
    public int getRequestOpCode() {
        return ZooDefs.OpCode.getACL;
    }

    public Stat getStat(){
        return stat;
    }
    
    public List<ACL> getAcl(){
        return acl;
    }
}
