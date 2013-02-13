/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.thriftzmq;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import java.util.UUID;
import org.jeromq.ZMQ;

/**
 *
 * @author wildfire
 */
public class TZMQTransportPool extends AbstractExecutionThreadService {

    private static final int POLL_TIMEOUT_MS = 100;

    private final ZMQ.Context context;
    private final TZMQTransportFactory backendFactory;
    private final TZMQTransportFactory clientFactory;
    private final String backEndpoint;

    private volatile boolean stop = false;

    private TZMQTransport frontend;
    private ZMQ.Socket backend;

    public TZMQTransportPool(TZMQTransportFactory factory) {
        this.context = factory.getContext();
        this.backendFactory = factory;
        this.backEndpoint = "inproc://tzmq_" + UUID.randomUUID().toString();
        this.clientFactory = new TZMQTransportFactory(context, backEndpoint, ZMQ.REQ, false);
    }

    public ZMQ.Context getContext() {
        return context;
    }

    @Override
    protected void startUp() throws Exception {
        this.stop = false;
        frontend = backendFactory.create();
        frontend.open();

        backend = context.socket(ZMQ.DEALER);
        backend.bind(backEndpoint);
    }

    @Override
    protected void run() throws Exception {
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
    protected void shutDown() throws Exception {
        frontend.close();
        backend.close();
    }

    @Override
    protected void triggerShutdown() {
        stop = true;
    }

    public TZMQTransport getClient() {
        return clientFactory.create();
    }

}
