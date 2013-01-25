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

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQServerTransport {

    private final ZMQ.Context context;
    private final String address;
    
    private ZMQ.Socket socket;

    public TZMQServerTransport(ZMQ.Context context, String address) {
        this.context = context;
        this.address = address;
    }

    public ZMQ.Context getContext() {
        return context;
    }

    public String getAddress() {
        return address;
    }

    public void listen(int socketType) {
        this.socket = context.socket(socketType);
        socket.bind(address);
    }

    public ZMQ.Socket getSocket() {
        return this.socket;
    }

    public void close() {
        if (socket != null) {
            socket.close();
        }
    }

}
