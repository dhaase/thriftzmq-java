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
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.jeromq.ZMQ;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQSimpleServer extends TZMQServer {

    public static class Args extends TZMQServer.AbstractServerArgs<Args> {

        public Args(TZMQServerTransport serverTransport) {
            super(serverTransport);
        }
        
    }

    private static final int POLL_TIMEOUT_MS = 1000;

    private ZMQ.Context context;
    private ZMQ.Socket socket;
    private volatile boolean stop = false;

    public TZMQSimpleServer(Args args) {
        super(args);
    }

    @Override
    protected void startUp() {
        this.stop = false;
        context = serverTransport.getContext();
        serverTransport.listen(ZMQ.REP);
    }

    @Override
    public void run() {
        socket = serverTransport.getSocket();
        ZMQ.Poller poller = context.poller(1);
        poller.register(socket, ZMQ.Poller.POLLIN);

        byte[] message;
        TProcessor processor = null;
        TMemoryInputTransport inputTransport = null;
        TMemoryBuffer outputTransport = null;
        TProtocol inputProtocol = null;
        TProtocol outputProtocol = null;

        while (!stop) {
            poller.poll(POLL_TIMEOUT_MS);
            if (poller.pollin(0)) {
                message = socket.recv(0);//TODO: Flags?
                inputTransport = new TMemoryInputTransport(message);
                inputProtocol = inputProtocolFactory.getProtocol(inputTransport);
                outputTransport = new TMemoryBuffer(0);//TODO: Optimize
                outputProtocol = outputProtocolFactory.getProtocol(outputTransport);
                processor = processorFactory.getProcessor(inputTransport);
                try {
                    processor.process(inputProtocol, outputProtocol);
                    byte[] rep = outputTransport.getArray();
                    int len = outputTransport.length();
                    socket.send(rep);//TODO: Don't send array tail
                } catch (TException ex) {
                    //TODO: Handle
                    //Logger.getLogger(TZMQSimpleServer.class.getName()).log(Level.SEVERE, null, ex);
                }
                inputTransport.close();
                outputTransport.close();
            }
        }
    }

    @Override
    protected void shutDown() {
        serverTransport.close();
    }

    @Override
    protected void triggerShutdown() {
        stop = true;
    }

}
