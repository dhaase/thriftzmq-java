/*
 * Copyright (C) 2013 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thriftzmq;

import java.nio.ByteBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQTransport extends TTransport {

    //Size for output buffer
    private static final int WRITE_BUFFER_SIZE = 4096;

    private final ZMQ.Context context;
    private final String address;
    private final int socketType;
    private final boolean bind;

    private ZMQ.Socket socket;
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
    private TMemoryInputTransport readBuffer = new TMemoryInputTransport();
    private boolean hasReceiveMore;

    public TZMQTransport(Context context, String address, int socketType, boolean bind) {
        this.context = context;
        this.address = address;
        this.socketType = socketType;
        this.bind = bind;
    }

    public Socket getSocket() {
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
        if (readBuffer != null) {
            int got = readBuffer.read(buf, off, len);
            if (got > 0) {
                return got;
            }
        }

        // Read another frame of data
        readFrame();

        return readBuffer.read(buf, off, len);
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
        //readFrame();

        //return readBuffer.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        readBuffer.consumeBuffer(len);
    }

    private void readFrame() {
        if (socket == null) {
            throw new IllegalStateException("Attempt to read from closed transport");
        }
        byte[] r = socket.recv(0);//TODO: Flags?
        hasReceiveMore = socket.hasReceiveMore();
        readBuffer.reset(r);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        do {
            if (writeBuffer.remaining() == 0) {
                sendBuffered(true);
            }
            int l = Math.min(writeBuffer.remaining(), len);
            writeBuffer.put(buf, off, l);
            off += l;
            len -= l;
        } while (len > 0);
    }

    @Override
    public void flush() throws TTransportException {
        sendBuffered(false);
        super.flush();
    }

    private void sendBuffered(boolean more) throws TTransportException {
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


