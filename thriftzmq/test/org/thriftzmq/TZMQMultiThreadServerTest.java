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
import org.apache.thrift.protocol.TCompactProtocol;
import org.jeromq.ZMQ;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.thriftzmq.test.Service1;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQMultiThreadServerTest {

    private static final String INPROC_ENDPOINT = "inproc://service1";

    private static ZMQ.Context context;

    public TZMQMultiThreadServerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        context = ZMQ.context();
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
    public void testServe() throws TException, InterruptedException {
        System.out.println("serve");
        TZMQMultiThreadServer server = createServer();
        server.startAndWait();
        TZMQTransport clientTransport = new TZMQTransport(context, INPROC_ENDPOINT, ZMQ.REQ, false);
        Service1.Client client = new Service1.Client(new TCompactProtocol(clientTransport));
        clientTransport.open();
        String s = "abcdABCD";
        String r = client.echo(s);
        assertEquals(s, r);
        server.stop();
    }

    private static TZMQMultiThreadServer createServer() {
        Service1Impl impl = new Service1Impl();
        TZMQTransportFactory socketFactory = new TZMQTransportFactory(context, INPROC_ENDPOINT, ZMQ.ROUTER, true);
        TZMQMultiThreadServer.Args args = new TZMQMultiThreadServer.Args(socketFactory, "backend");
        args.protocolFactory(new TCompactProtocol.Factory())
                .processor(new Service1.Processor<Service1.Iface>(impl));
        TZMQMultiThreadServer instance = new TZMQMultiThreadServer(args);
        return instance;
    }

}
