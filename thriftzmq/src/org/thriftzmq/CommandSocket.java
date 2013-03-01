/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.thriftzmq;

import java.util.concurrent.atomic.AtomicLong;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;

/**
 *
 * @author wildfire
 */
class CommandSocket {
    
    private static final AtomicLong socketId = new AtomicLong();

    public static final byte STOP = 1;

    private final ZMQ.Context context;
    private final String endpoint;

    private ZMQ.Socket socket;

    public CommandSocket(ZMQ.Context context) {
        this.context = context;
        endpoint = "inproc://TZMQ_CONTROL_" + Long.toHexString(socketId.incrementAndGet());
    }

    public void open() {
        socket = context.socket(ZMQ.PULL);
        socket.bind(endpoint);
    }

    public void close() {
        socket.close();
    }

    public Socket getSocket() {
        return socket;
    }

    public byte recvCommand() {
        byte[] msg = socket.recv(0);
        return msg[0];
    }

    public void sendCommand(byte cmd) {
        byte[] msg = new byte[1];
        msg[0] = cmd;
        ZMQ.Socket peer = context.socket(ZMQ.PUSH);
        peer.connect(endpoint);
        peer.send(msg, 0);
        peer.close();
    }
}
