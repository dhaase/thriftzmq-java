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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.transport.TTransport;
import org.jeromq.ZMQ;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQClientPool extends AbstractExecutionThreadService {

    private static final int POLL_TIMEOUT_MS = 100;
    private static final AtomicLong socketId = new AtomicLong();

    private final ZMQ.Context context;
    private final String frontEndpoint;
    private final String backendAddress;
    private final TransportSocketFactory clientFactory;

    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private CommandSocket commandSocket;

    public TZMQClientPool(ZMQ.Context context, String address) {
        this.context = context;
        this.backendAddress = address;
        this.frontEndpoint = "inproc://TZMQ_POOL_" + Long.toHexString(socketId.incrementAndGet());
        this.clientFactory = new TransportSocketFactory(context, frontEndpoint, ZMQ.REQ, false);
    }

    public ZMQ.Context getContext() {
        return context;
    }

    @Override
    protected void startUp() throws Exception {
        frontend = context.socket(ZMQ.ROUTER);
        frontend.bind(frontEndpoint);

        backend = context.socket(ZMQ.DEALER);
        backend.connect(backendAddress);

        commandSocket = new CommandSocket(context);
        commandSocket.open();
    }

    @Override
    protected void run() throws Exception {
        ProxyLoop proxyLoop = new ProxyLoop(context, frontend, backend, commandSocket);
        proxyLoop.run();
    }

    @Override
    protected void shutDown() throws Exception {
        //XXX: For now force closing sockets to prevent hang on shutdown
        frontend.setLinger(0);
        backend.setLinger(0);
        frontend.close();
        backend.close();
    }

    @Override
    protected void triggerShutdown() {
        commandSocket.sendCommand(CommandSocket.STOP);
    }

    public TTransport getClient() {
        return clientFactory.create();
    }

}
