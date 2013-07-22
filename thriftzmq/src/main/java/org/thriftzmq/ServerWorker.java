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
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jeromq.ZMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Vyacheslav Baranov
 */
class ServerWorker extends AbstractExecutionThreadService {

    private static final Logger logger = LoggerFactory.getLogger(ServerWorker.class);

    private static final int POLL_TIMEOUT_MS = 1000;

    private final TZMQSocketFactory socketFactory;
    private final TProtocolFactory inputProtocolFactory;
    private final TProtocolFactory outputProtocolFactory;
    private final TProcessorFactory processorFactory;

    private TZMQSocket transportSocket;
    private CommandSocket commandSocket;

    public ServerWorker(TZMQSocketFactory socketFactory,
            TProtocolFactory inputProtocolFactory, TProtocolFactory outputProtocolFactory,
            TProcessorFactory processorFactory) {
        this.socketFactory = socketFactory;
        this.inputProtocolFactory = inputProtocolFactory;
        this.outputProtocolFactory = outputProtocolFactory;
        this.processorFactory = processorFactory;
    }

    @Override
    protected void startUp() throws Exception {
        ZMQ.Context context = socketFactory.getContext();
        transportSocket = socketFactory.create();
        commandSocket = new CommandSocket(context);
        transportSocket.open();
        commandSocket.open();
    }

    @Override
    protected void run() throws Exception {
        ZMQ.Context context = socketFactory.getContext();
        ZMQ.Poller poller = context.poller(2);
        poller.register(transportSocket.getSocket(), ZMQ.Poller.POLLIN);
        poller.register(commandSocket.getSocket(), ZMQ.Poller.POLLIN);

        while (true) {
            poller.poll(POLL_TIMEOUT_MS);
            if (poller.pollin(0)) {
                TProtocol inputProtocol = inputProtocolFactory.getProtocol(transportSocket);
                TProtocol outputProtocol = outputProtocolFactory.getProtocol(transportSocket);
                TProcessor processor = processorFactory.getProcessor(transportSocket);
                try {
                    processor.process(inputProtocol, outputProtocol);
                } catch (TException ex) {
                    logger.error("Exception in request processor:",  ex);
                    throw ex;
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
        try {
            transportSocket.close();
            commandSocket.close();
        } catch (RuntimeException ex) {
            logger.warn("Exception during shutdown", ex);
            throw ex;
        }
    }

    @Override
    protected void triggerShutdown() {
        commandSocket.sendCommand(CommandSocket.STOP);
    }

}
