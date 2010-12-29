package org.apache.zookeeper.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

/**
 * This class allows us to pass the headers and the relevant records around.
 */
public class Packet {
    private static final Logger LOG = Logger.getLogger(Packet.class);
    
    public RequestHeader requestHeader;

    public ReplyHeader replyHeader;

    Record request;

    public Record response;

    ByteBuffer bb;

    /** Client's view of the path (may differ due to chroot) **/
    public String clientPath;
    /** Servers's view of the path (may differ due to chroot) **/
    public String serverPath;

    public boolean finished;

    public AsyncCallback cb;

    public Object ctx;

    public WatchRegistration watchRegistration;

    public Packet(RequestHeader requestHeader, ReplyHeader replyHeader, Record request,
            Record response, WatchRegistration watchRegistration) {
        this.requestHeader = requestHeader;
        this.replyHeader = replyHeader;
        this.request = request;
        this.response = response;

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            boa.writeInt(-1, "len"); // We'll fill this in later
            if (requestHeader != null) {
                requestHeader.serialize(boa, "header");
            }
            if (request instanceof ConnectRequest) {
                request.serialize(boa, "connect");
            } else if (request != null) {
                request.serialize(boa, "request");
            }
            baos.close();
            this.bb = ByteBuffer.wrap(baos.toByteArray());
            this.bb.putInt(this.bb.capacity() - 4);
            this.bb.rewind();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }

        this.watchRegistration = watchRegistration;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("clientPath:" + clientPath);
        sb.append(" serverPath:" + serverPath);
        sb.append(" finished:" + finished);

        sb.append(" header:: " + requestHeader);
        sb.append(" replyHeader:: " + replyHeader);
        sb.append(" request:: " + request);
        sb.append(" response:: " + response);

        // jute toString is horrible, remove unnecessary newlines
        return sb.toString().replaceAll("\r*\n+", " ");
    }
}