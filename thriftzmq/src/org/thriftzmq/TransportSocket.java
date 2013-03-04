/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.thriftzmq;

import java.nio.ByteBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jeromq.ZMQ;

/**
 *
 * @author wildfire
 */
class TransportSocket extends TTransport {

    //Size for output buffer
    private static final int WRITE_BUFFER_SIZE = 4096;

    private final ZMQ.Context context;
    private final String address;
    private final int socketType;
    private final boolean bind;

    private ZMQ.Socket socket;
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
    private final TMemoryInputTransport readBuffer = new TMemoryInputTransport();
    private boolean hasReceiveMore;

    public TransportSocket(ZMQ.Context context, String address, int socketType, boolean bind) {
        this.context = context;
        this.address = address;
        this.socketType = socketType;
        this.bind = bind;
    }

    public ZMQ.Socket getSocket() {
        return socket;
    }

    @Override
    public boolean isOpen() {
        return socket != null;//TODO: Separate flag?
    }

    @Override
    public void open() {
        socket = context.socket(socketType);
        if (bind) {
            socket.bind(address);
        } else {
            socket.connect(address);
        }
    }

    @Override
    public void close() {
        if (socket != null) {
            //XXX: For now force closing socket to prevent hang on shutdown
            socket.setLinger(0);
            socket.close();
        }
        socket = null;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        do {
            int got = readBuffer.read(buf, off, len);
            if (got > 0) {
                return got;
            }
        } while (readFrame());

        //No more data
        return 0;
    }

    @Override
    public byte[] getBuffer() {
        return readBuffer.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return readBuffer.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        if (readBuffer != null) {
            int remaining = readBuffer.getBytesRemainingInBuffer();
            return remaining;
        }
        return 0;
    }

    @Override
    public void consumeBuffer(int len) {
        readBuffer.consumeBuffer(len);
    }

    private boolean readFrame() {
        if (socket == null) {
            throw new IllegalStateException("Attempt to read from closed transport");
        }
        byte[] r = socket.recv(0);//TODO: Flags?
        hasReceiveMore = socket.hasReceiveMore();
        readBuffer.reset(r);
        return true;//Ok next frame is read
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        do {
            if (writeBuffer.remaining() == 0) {
                sendCurrentBuffer(true);
            }
            int l = Math.min(writeBuffer.remaining(), len);
            writeBuffer.put(buf, off, l);
            off += l;
            len -= l;
        } while (len > 0);
    }

    @Override
    public void flush() throws TTransportException {
        sendCurrentBuffer(false);
    }

    private void sendCurrentBuffer(boolean more) throws TTransportException {
        if (socket == null) {
            throw new TTransportException("Attempt to write to closed transport");
        }
        //Buffer must be copied, because jeromq reads it asynchroniously
        byte[] data;
        int l = writeBuffer.position();
        data = new byte[l];
        writeBuffer.position(0);
        writeBuffer.get(data);
        socket.send(data, more ? ZMQ.SNDMORE : 0);
        writeBuffer.clear();
    }
}
