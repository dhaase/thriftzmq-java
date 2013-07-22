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

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.Executor;
import org.jeromq.ZMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQSimpleServer extends TZMQServer {

    public static class Args extends TZMQServer.AbstractServerArgs<Args> {

        public Args(ZMQ.Context context, String address) {
            super(context, address);
        }

    }

    private static final Logger logger = LoggerFactory.getLogger(TZMQSimpleServer.class);

    private static final int POLL_TIMEOUT_MS = 1000;

    private final ServerWorker worker;

    public TZMQSimpleServer(Args args) {
        super(args);
        TZMQSocketFactory socketFactory = new TZMQSocketFactory(context, address, ZMQ.REP, true);
        worker = new ServerWorker(socketFactory, inputProtocolFactory, outputProtocolFactory, processorFactory);
    }

    @Override
    public ListenableFuture<State> start() {
        return worker.start();
    }

    @Override
    public State startAndWait() {
        return worker.startAndWait();
    }

    @Override
    public boolean isRunning() {
        return worker.isRunning();
    }

    @Override
    public State state() {
        return worker.state();
    }

    @Override
    public ListenableFuture<State> stop() {
        return worker.stop();
    }

    @Override
    public State stopAndWait() {
        return worker.stopAndWait();
    }

    @Override
    public void addListener(Listener listener, Executor executor) {
        worker.addListener(listener, executor);
    }

    @Override
    public Throwable failureCause() {
        return worker.failureCause();
    }


}
