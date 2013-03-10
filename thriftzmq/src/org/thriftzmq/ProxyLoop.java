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

import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;

/**
 *
 * @author Vyacheslav Baranov
 */
class ProxyLoop implements Runnable {

    private static final int POLL_TIMEOUT_MS = 1000;

    private final ZMQ.Context context;
    private final ZMQ.Socket socket1;
    private final ZMQ.Socket socket2;
    private final CommandSocket commandSocket;

    public ProxyLoop(Context context, Socket socket1, Socket socket2, CommandSocket commandSocket) {
        this.context = context;
        this.socket1 = socket1;
        this.socket2 = socket2;
        this.commandSocket = commandSocket;
    }

    @Override
    public void run() {
        ZMQ.Poller poller = context.poller(3);
        poller.register(socket1, ZMQ.Poller.POLLIN);
        poller.register(socket2, ZMQ.Poller.POLLIN);
        poller.register(commandSocket.getSocket(), ZMQ.Poller.POLLIN);

        byte[] message;
        boolean more;

        while (true) {
            poller.poll(POLL_TIMEOUT_MS);
            if (poller.pollin(0)) {
                do {
                    message = socket1.recv(0);
                    more = socket1.hasReceiveMore();
                    socket2.send(message, more ? ZMQ.SNDMORE : 0);
                } while (more);
            }
            if (poller.pollin(1)) {
                do {
                    message = socket2.recv(0);
                    more = socket2.hasReceiveMore();
                    socket1.send(message, more ? ZMQ.SNDMORE : 0);
                } while (more);
            }
            if (poller.pollin(2)) {
                byte cmd = commandSocket.recvCommand();
                if (cmd == CommandSocket.STOP) {
                    break;
                }
            }
        }
    }

}
