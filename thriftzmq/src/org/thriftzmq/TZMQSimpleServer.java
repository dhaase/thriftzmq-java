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

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.jeromq.ZMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQSimpleServer extends TZMQServer {

    public static class Args extends TZMQServer.AbstractServerArgs<Args> {

        public Args(TZMQTransportFactory socketFactory) {
            super(socketFactory);
        }
        
    }

    private static final Logger logger = LoggerFactory.getLogger(TZMQSimpleServer.class);

    private static final int POLL_TIMEOUT_MS = 1000;

    private ZMQ.Context context;
    private TZMQTransport socket;
    private CommandSocket commandSocket;

    public TZMQSimpleServer(Args args) {
        super(args);
    }

    @Override
    protected void startUp() {
        context = transportFactory.getContext();
        socket = transportFactory.create();
        commandSocket = new CommandSocket(context);
        socket.open();
        commandSocket.open();
    }

    @Override
    public void run() {
        ZMQ.Poller poller = context.poller(2);
        poller.register(socket.getSocket(), ZMQ.Poller.POLLIN);
        poller.register(commandSocket.getSocket(), ZMQ.Poller.POLLIN);

        byte[] message;

        while (true) {
            poller.poll(POLL_TIMEOUT_MS);
            if (poller.pollin(0)) {
                TProtocol inputProtocol = inputProtocolFactory.getProtocol(socket);
                TProtocol outputProtocol = outputProtocolFactory.getProtocol(socket);
                TProcessor processor = processorFactory.getProcessor(socket);
                try {
                    processor.process(inputProtocol, outputProtocol);
                    //TODO: flush()?
                } catch (TException ex) {
                    //TODO: Handle
                    logger.error("Exception in request processor: {}",  ex);
                }
            }
            if (poller.pollin(1)) {
                byte cmd = commandSocket.recvCommand();
                if (cmd == CommandSocket.STOP) {
                    break;
                }
            }
        }
    }

    @Override
    protected void shutDown() {
        //XXX: For now force closing socket to prevent hang on shutdown
        socket.getSocket().setLinger(0);
        socket.close();
    }

    @Override
    protected void triggerShutdown() {
        commandSocket.sendCommand(CommandSocket.STOP);
    }

}
