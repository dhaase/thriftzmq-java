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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.jeromq.ZMQ;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thriftzmq.test.Service1;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQMultiThreadServerTest {

    private static final Logger logger = LoggerFactory.getLogger(TZMQMultiThreadServerTest.class);

    private static final String INPROC_ENDPOINT = "inproc://service1";
    private static final String TCP_ENDPOINT = "tcp://127.0.0.1:23541";

    private static ZMQ.Context context;

    public TZMQMultiThreadServerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        context = ZMQ.context(1);
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of serve method, of class TZMQSimpleServer.
     */
    @Test
    public void testEcho() throws TException, InterruptedException {
        System.out.println("echo");
        TZMQMultiThreadServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        TZMQTransport clientTransport = new TZMQTransport(context, TCP_ENDPOINT, ZMQ.REQ, false);
        Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
        clientTransport.open();
        String s = "abcdABCD";
        String r = client.echo(s);
        assertEquals(s, r);
        server.stopAndWait();
    }

    /**
     * Test of echo method with long argument
     */
    @Test
    public void testEchoLong() throws TException, InterruptedException {
        System.out.println("echoLong");
        TZMQMultiThreadServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        TZMQTransport clientTransport = new TZMQTransport(context, TCP_ENDPOINT, ZMQ.REQ, false);
        Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
        clientTransport.open();
        //String s = "abcdABCD";
        int l = 1024 * 1024;
        char c[] = new char[l];
        Random rand = new Random(12345);
        for (int i = 0; i < l; i++) {
            c[i] = (char) (rand.nextInt(0x80 - 0x20) + 0x20);
        }
        String s = new String(c);
        String r = client.echo(s);
        assertEquals(s, r);
        server.stopAndWait();
    }

    private static class ClientWorker implements Callable<Void> {

        private final String endpoint;

        public ClientWorker(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public Void call() throws Exception {
            TZMQTransport clientTransport = new TZMQTransport(context, TCP_ENDPOINT, ZMQ.REQ, false);
            Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
            clientTransport.open();
            String s = "abcdABCD";
            String r = client.echo(s);
            assertEquals(s, r);
            return null;
        }
    }

    @Test
    public void testEchoMultiThreaded() throws TException, InterruptedException, ExecutionException {
        System.out.println("echoMultiThreaded");
        TZMQMultiThreadServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        int cnt = 100;
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        List<ListenableFuture<Void>> fl = new ArrayList<ListenableFuture<Void>>();
        for (int i = 0; i < 100; i++) {
            fl.add(executor.submit(new ClientWorker(TCP_ENDPOINT)));
        }
        Futures.successfulAsList(fl).get();
        server.stopAndWait();
    }

    private static class PoolClientWorker implements Callable<Void> {

        private final TZMQTransportPool pool;
        private final static AtomicInteger sendCount = new AtomicInteger();
        private final static AtomicInteger recvCount = new AtomicInteger();

        public PoolClientWorker(TZMQTransportPool pool) {
            this.pool = pool;
        }

        @Override
        public Void call() throws Exception {
            TZMQTransport clientTransport = pool.getClient();
            Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
            clientTransport.open();
            try {
                String s = "abcdABCD";
                sendCount.incrementAndGet();
                String r = client.echo(s);
                assertEquals(s, r);
                recvCount.incrementAndGet();
            } catch (Exception ex) {
                recvCount.incrementAndGet();
                logger.error("Error invoking server:", ex);
                throw ex;
            } finally {
                clientTransport.close();
            }
            return null;
        }
    }

    @Test
    public void testEchoPooled() throws Exception {
        System.out.println("testEchoPooled");
        TZMQMultiThreadServer server = createServer(TCP_ENDPOINT);
        int initialCnt = Service1Impl.ECHO_INVOKE_COUNT.get();
        server.startAndWait();
        int cnt = 100;
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        List<ListenableFuture<Void>> fl = new ArrayList<ListenableFuture<Void>>();
        TZMQTransportPool pool = new TZMQTransportPool(new TZMQTransportFactory(context, TCP_ENDPOINT, ZMQ.DEALER, false));
        pool.startAndWait();
        try {
            for (int i = 0; i < cnt; i++) {
                fl.add(executor.submit(new PoolClientWorker(pool)));
            }
            Futures.successfulAsList(fl).get(5000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            //XXX: This code is to debug rare hang when no response is received in one of clients
            int invokedCnt = Service1Impl.ECHO_INVOKE_COUNT.get() - initialCnt;
            int sendCnt = PoolClientWorker.sendCount.get();
            int recvCnt = PoolClientWorker.recvCount.get();
            logger.warn("Timed out while waiting for workers. Sent : {}, Received: {}, Server invoked: {}",
                    new Object[] {sendCnt, recvCnt, invokedCnt});
            throw ex;
        } finally {
            pool.stopAndWait();
            server.stopAndWait();
        }
    }

    /**
     * Test of voidMethod method
     */
    @Test
    public void testVoidMethod() throws TException, InterruptedException {
        System.out.println("voidMethod");
        TZMQMultiThreadServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        TZMQTransport clientTransport = new TZMQTransport(context, TCP_ENDPOINT, ZMQ.REQ, false);
        Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
        clientTransport.open();
        String s = "abcdABCD";
        client.voidMethod(s);
        server.stopAndWait();
    }

    private static TZMQMultiThreadServer createServer(String endpoint) {
        Service1Impl impl = new Service1Impl();
        TZMQMultiThreadServer.Args args = new TZMQMultiThreadServer.Args(context, endpoint);
        args.protocolFactory(new TCompactProtocol.Factory())
                .processor(new Service1.Processor<Service1.Iface>(impl))
                .threadCount(5);
        TZMQMultiThreadServer instance = new TZMQMultiThreadServer(args);
        return instance;
    }

}
