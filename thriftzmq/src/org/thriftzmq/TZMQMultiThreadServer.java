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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQMultiThreadServer extends TZMQServer {

    public static class Args extends TZMQServer.AbstractServerArgs<Args> {

        protected int threadCount = 1;
        protected ThreadGroup threadGroup = null;
        protected String serviceName;

        public Args(TZMQServerTransport serverTransport, String serviceName) {
            super(serverTransport);
            this.serviceName = serviceName;
        }

        public Args threadCount(int threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        public Args threadGroup(ThreadGroup threadGroup) {
            this.threadGroup = threadGroup;
            return this;
        }
    }

    private static final int POLL_TIMEOUT_MS = 1000;

    private String backEndpoint;
    private ZMQ.Context context;
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private volatile boolean stop = false;
    private TZMQSimpleServer[] workers = null;

    public TZMQMultiThreadServer(Args args) {
        super(args);
        backEndpoint = "inproc://" + args.serviceName;
        workers = new TZMQSimpleServer[args.threadCount];
        this.context = args.serverTransport.getContext();
        for (int i = 0; i < args.threadCount; i++) {
            TZMQServerTransport workerTransport = new TZMQServerTransport(context, backEndpoint) {

                private ZMQ.Socket socket;

                @Override
                public void listen(int socketType) {
                    this.socket = context.socket(socketType);
                    socket.connect(getAddress());
                }

                @Override
                public Socket getSocket() {
                    return this.socket;
                }

            };
            TZMQSimpleServer.Args workerArgs = new TZMQSimpleServer.Args(workerTransport);
            workerArgs.inputProtocolFactory(args.inputProtocolFactory)
                    .outputProtocolFactory(args.outputProtocolFactory)
                    .processorFactory(args.processorFactory);
            workers[i] = new TZMQSimpleServer(workerArgs);
        }
    }

    @Override
    protected void startUp() throws InterruptedException, ExecutionException {
        this.stop = false;
        context = serverTransport.getContext();
        serverTransport.listen(ZMQ.ROUTER);
        frontend = serverTransport.getSocket();
        backend = context.socket(ZMQ.DEALER);
        backend.bind(backEndpoint);
        ListenableFuture<List<State>> f = Futures.successfulAsList(Iterables.transform(Arrays.asList(workers),
                new Function<TZMQSimpleServer, ListenableFuture<State>>() {

                    @Override
                    public ListenableFuture<State> apply(TZMQSimpleServer input) {
                        return input.start();
                    }
                    
                }));
        List<State> r = f.get();
    }

    @Override
    public void run() {
        ZMQ.Poller poller = context.poller(2);
        poller.register(frontend, ZMQ.Poller.POLLIN);
        poller.register(backend, ZMQ.Poller.POLLIN);

        byte[] message;
        TProcessor processor = null;
        TMemoryInputTransport inputTransport = null;
        TMemoryBuffer outputTransport = null;
        TProtocol inputProtocol = null;
        TProtocol outputProtocol = null;

        boolean more;

        while (!stop) {
            poller.poll(POLL_TIMEOUT_MS);
            if (poller.pollin(0)) {
                do {
                    message = frontend.recv(0);//TODO: Flags?
                    more = frontend.hasReceiveMore();
                    backend.send(message, more ? ZMQ.SNDMORE : 0);
                } while (more);
            }
            if (poller.pollin(1)) {
                do {
                    message = backend.recv(0);//TODO: Flags?
                    more = backend.hasReceiveMore();
                    frontend.send(message, more ? ZMQ.SNDMORE : 0);
                } while (more);
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
