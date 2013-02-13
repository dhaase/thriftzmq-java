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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.jeromq.ZMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQMultiThreadServer extends TZMQServer {

    public static class Args extends TZMQServer.AbstractServerArgs<Args> {

        protected int threadCount = 1;
        protected ThreadGroup threadGroup = null;
        protected String serviceName;

        public Args(TZMQTransportFactory transportFactory, String serviceName) {
            super(transportFactory);
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

    private static final Logger logger = LoggerFactory.getLogger(TZMQMultiThreadServer.class);

    private static final int POLL_TIMEOUT_MS = 100;

    private int threadCount;
    private String backEndpoint;
    private ZMQ.Context context;
    private TZMQTransport frontend;
    private ZMQ.Socket backend;
    private volatile boolean stop = false;
    private TZMQSimpleServer[] workers = null;

    public TZMQMultiThreadServer(Args args) {
        super(args);
        backEndpoint = "inproc://" + args.serviceName;
        this.threadCount = args.threadCount;
    }

    @Override
    protected void startUp() throws InterruptedException, ExecutionException {
        this.stop = false;

        //Create workers
        workers = new TZMQSimpleServer[threadCount];
        this.context = transportFactory.getContext();
        for (int i = 0; i < threadCount; i++) {
            TZMQTransportFactory workerTransport = new TZMQTransportFactory(context, backEndpoint, ZMQ.REP, false);
            TZMQSimpleServer.Args workerArgs = new TZMQSimpleServer.Args(workerTransport);
            workerArgs.inputProtocolFactory(inputProtocolFactory)
                    .outputProtocolFactory(outputProtocolFactory)
                    .processorFactory(processorFactory);
            workers[i] = new TZMQSimpleServer(workerArgs);
        }

        //Create sockets
        context = transportFactory.getContext();
        frontend = transportFactory.create();
        frontend.open();
        backend = context.socket(ZMQ.DEALER);
        backend.bind(backEndpoint);

        //Start workers
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
        poller.register(frontend.getSocket(), ZMQ.Poller.POLLIN);
        poller.register(backend, ZMQ.Poller.POLLIN);

        byte[] message;
        boolean more;

        while (!stop) {
            poller.poll(POLL_TIMEOUT_MS);
            if (poller.pollin(0)) {
                do {
                    message = frontend.getSocket().recv(0);//TODO: Flags?
                    more = frontend.getSocket().hasReceiveMore();
                    backend.send(message, more ? ZMQ.SNDMORE : 0);
                } while (more);
            }
            if (poller.pollin(1)) {
                do {
                    message = backend.recv(0);//TODO: Flags?
                    more = backend.hasReceiveMore();
                    frontend.getSocket().send(message, more ? ZMQ.SNDMORE : 0);
                } while (more);
            }
        }
    }

    @Override
    protected void shutDown() {
        //TODO: Graceful shutdown
        ListenableFuture<List<State>> f = Futures.successfulAsList(Iterables.transform(Arrays.asList(workers),
                new Function<TZMQSimpleServer, ListenableFuture<State>>() {

                    @Override
                    public ListenableFuture<State> apply(TZMQSimpleServer input) {
                        return input.stop();
                    }

                }));
        try {
            f.get(POLL_TIMEOUT_MS * 10, TimeUnit.MILLISECONDS);//TODO: Fix
        } catch (InterruptedException ex) {
            logger.warn("Interrupted on shutdown", ex);
        } catch (ExecutionException ex) {
            logger.warn("Exception while stopping workers", ex);
        } catch (TimeoutException ex) {
            logger.warn("Timeout on shutdown", ex);
        }
        frontend.getSocket().setLinger(0);
        frontend.close();
        backend.close();

        workers = null;
    }

    @Override
    protected void triggerShutdown() {
        stop = true;
    }

}
