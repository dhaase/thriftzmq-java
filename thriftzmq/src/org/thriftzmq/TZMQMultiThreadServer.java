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
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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

        public Args(ZMQ.Context context, String address) {
            super(context, address);
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

    private class Delegate extends AbstractExecutionThreadService {

        private static final int POLL_TIMEOUT_MS = 1000;

        private ZMQ.Socket frontend;
        private ZMQ.Socket backend;
        private CommandSocket commandSocket;
        private ServerWorker[] workers = null;

        @Override
        protected void startUp() throws InterruptedException, ExecutionException {

            //Create workers
            workers = new ServerWorker[threadCount];
            for (int i = 0; i < threadCount; i++) {
                workers[i] = new ServerWorker(workerSocketFactory, inputProtocolFactory, outputProtocolFactory, processorFactory);
            }

            //Create sockets
            frontend = context.socket(ZMQ.ROUTER);
            frontend.bind(address);
            backend = context.socket(ZMQ.DEALER);
            backend.bind(backEndpoint);
            commandSocket = new CommandSocket(context);
            commandSocket.open();

            //Start workers
            ListenableFuture<List<State>> f = Futures.successfulAsList(Iterables.transform(Arrays.asList(workers),
                    new Function<ServerWorker, ListenableFuture<State>>() {

                        @Override
                        public ListenableFuture<State> apply(ServerWorker input) {
                            return input.start();
                        }

                    }));
            List<State> r = f.get();
        }

        @Override
        public void run() {
            ProxyLoop proxyLoop = new ProxyLoop(context, frontend, backend, commandSocket);
            proxyLoop.run();
        }

        @Override
        protected void shutDown() {
            //TODO: Graceful shutdown
            ListenableFuture<List<State>> f = Futures.successfulAsList(Iterables.transform(Arrays.asList(workers),
                    new Function<ServerWorker, ListenableFuture<State>>() {

                        @Override
                        public ListenableFuture<State> apply(ServerWorker input) {
                            return input.stop();
                        }

                    }));
            try {
                f.get(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                logger.warn("Interrupted on shutdown", ex);
            } catch (ExecutionException ex) {
                logger.warn("Exception while stopping workers", ex);
            } catch (TimeoutException ex) {
                logger.warn("Timeout on shutdown", ex);
            }
            //XXX: For now force closing sockets to prevent hang on shutdown
            frontend.setLinger(0);
            backend.setLinger(0);
            commandSocket.getSocket().setLinger(0);
            frontend.close();
            backend.close();
            commandSocket.close();

            workers = null;
        }

        @Override
        protected void triggerShutdown() {
            commandSocket.sendCommand(CommandSocket.STOP);
        }

        @Override
        protected String getServiceName() {
            return "TZMQMultiThreadServer.Delegate";
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(TZMQMultiThreadServer.class);

    private static final AtomicLong socketId = new AtomicLong();

    private final TZMQSocketFactory workerSocketFactory;
    private final int threadCount;
    private final String backEndpoint;
    private final Delegate delegate;

    public TZMQMultiThreadServer(Args args) {
        super(args);
        backEndpoint = "inproc://TZMQ_MTSERVER_" + Long.toHexString(socketId.incrementAndGet());
        this.workerSocketFactory = new TZMQSocketFactory(context, backEndpoint, ZMQ.REP, false);
        this.threadCount = args.threadCount;
        this.delegate = new Delegate();
    }

    @Override
    public ListenableFuture<State> start() {
        return delegate.start();
    }

    @Override
    public State startAndWait() {
        return delegate.startAndWait();
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public State state() {
        return delegate.state();
    }

    @Override
    public ListenableFuture<State> stop() {
        return delegate.stop();
    }

    @Override
    public State stopAndWait() {
        return delegate.stopAndWait();
    }

    @Override
    public void addListener(Listener listener, Executor executor) {
        delegate.addListener(listener, executor);
    }

}
