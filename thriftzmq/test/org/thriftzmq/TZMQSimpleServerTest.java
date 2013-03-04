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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
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
public class TZMQSimpleServerTest {

    private static final Logger logger = LoggerFactory.getLogger(TZMQSimpleServerTest.class);

    private static final String INPROC_ENDPOINT = "inproc://service1";
    private static final String TCP_ENDPOINT = "tcp://127.0.0.1:23541";

    private static ZMQ.Context context;

    public TZMQSimpleServerTest() {
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
     * Test of echo method
     */
    @Test
    public void testEcho() throws TException, InterruptedException {
        System.out.println("echo");
        TZMQSimpleServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        TTransport clientTransport = TZMQClientFactory.create(context, TCP_ENDPOINT);
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
        TZMQSimpleServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        TTransport clientTransport = TZMQClientFactory.create(context, TCP_ENDPOINT);
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

    private static class PoolClientWorker implements Callable<Void> {

        private final TZMQClientPool pool;

        public PoolClientWorker(TZMQClientPool pool) {
            this.pool = pool;
        }

        @Override
        public Void call() throws Exception {
            TTransport clientTransport = pool.getClient();
            Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
            clientTransport.open();
            try {
                String s = "abcdABCD"  + new Random().nextInt(1000000);
                String r = client.echo(s);
                assertEquals(s, r);
            } catch (Exception ex) {
                logger.error("Error invoking server: {}", ex);
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
        TZMQSimpleServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        int cnt = 100;
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        List<ListenableFuture<Void>> fl = new ArrayList<ListenableFuture<Void>>();
        TZMQClientPool pool = new TZMQClientPool(context, TCP_ENDPOINT);
        pool.startAndWait();
        try {
            for (int i = 0; i < cnt; i++) {
                fl.add(executor.submit(new PoolClientWorker(pool)));
            }
            Futures.successfulAsList(fl).get(5000, TimeUnit.MILLISECONDS);
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
        TZMQSimpleServer server = createServer(TCP_ENDPOINT);
        server.startAndWait();
        TTransport clientTransport = TZMQClientFactory.create(context, TCP_ENDPOINT);
        Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
        clientTransport.open();
        String s = "abcdABCD";
        client.voidMethod(s);
        server.stopAndWait();
    }

    private static TZMQSimpleServer createServer(String endpoint) {
        Service1Impl impl = new Service1Impl();
        TZMQSimpleServer.Args args = new TZMQSimpleServer.Args(context, endpoint);
        args.protocolFactory(new TCompactProtocol.Factory())
                .processor(new Service1.Processor<Service1.Iface>(impl));
        TZMQSimpleServer instance = new TZMQSimpleServer(args);
        return instance;
    }

}
