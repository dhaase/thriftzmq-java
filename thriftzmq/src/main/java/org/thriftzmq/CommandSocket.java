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

import java.util.concurrent.atomic.AtomicLong;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;

/**
 *
 * @author Vyacheslav Baranov
 */
class CommandSocket {

    private static final AtomicLong socketId = new AtomicLong();

    public static final byte STOP = 1;

    private final ZMQ.Context context;
    private final String endpoint;

    private ZMQ.Socket socket;

    public CommandSocket(ZMQ.Context context) {
        this.context = context;
        endpoint = "inproc://TZMQ_COMMAND_" + Long.toHexString(socketId.incrementAndGet());
    }

    /**
     * Opens command socket.
     *
     * This method must be called on owning thread.
     *
     */
    public void open() {
        socket = context.socket(ZMQ.PULL);
        socket.bind(endpoint);
    }

    /**
     * Closes command socket.
     *
     * This method must be called on owning thread.
     */
    public void close() {
        socket.close();
    }

    /**
     * Retrieves underlying ZeroMQ socket.
     *
     * @return underlying ZeroMQ socket
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * Retrieves next command from the socket.
     *
     * This method must be called on owning thread.
     *
     * @return next command
     */
    public byte recvCommand() {
        byte[] msg = socket.recv(0);
        return msg[0];
    }

    /**
     * Sends a command to the socket.
     *
     * Creates peer socket and sends specified command to it.
     *
     * @param cmd command to send
     */
    public void sendCommand(byte cmd) {
        byte[] msg = new byte[1];
        msg[0] = cmd;
        ZMQ.Socket peer = context.socket(ZMQ.PUSH);
        peer.connect(endpoint);
        peer.send(msg, 0);
        peer.close();
    }
}
