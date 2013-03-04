/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.thriftzmq;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.transport.TTransport;
import org.jeromq.ZMQ;

/**
 *
 * @author wildfire
 */
public class TZMQClientPool extends AbstractExecutionThreadService {

    private static final int POLL_TIMEOUT_MS = 100;
    private static final AtomicLong socketId = new AtomicLong();

    private final ZMQ.Context context;
    private final String frontEndpoint;
    private final String backendAddress;
    private final TransportSocketFactory clientFactory;

    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private CommandSocket commandSocket;

    public TZMQClientPool(ZMQ.Context context, String address) {
        this.context = context;
        this.backendAddress = address;
        this.frontEndpoint = "inproc://TZMQ_POOL_" + Long.toHexString(socketId.incrementAndGet());
        this.clientFactory = new TransportSocketFactory(context, frontEndpoint, ZMQ.REQ, false);
    }

    public ZMQ.Context getContext() {
        return context;
    }

    @Override
    protected void startUp() throws Exception {
        frontend = context.socket(ZMQ.ROUTER);
        frontend.bind(frontEndpoint);

        backend = context.socket(ZMQ.DEALER);
        backend.connect(backendAddress);

        commandSocket = new CommandSocket(context);
        commandSocket.open();
    }

    @Override
    protected void run() throws Exception {
        ZMQ.Poller poller = context.poller(3);
        poller.register(frontend, ZMQ.Poller.POLLIN);
        poller.register(backend, ZMQ.Poller.POLLIN);
        poller.register(commandSocket.getSocket(), ZMQ.Poller.POLLIN);

        byte[] message;
        boolean more;

        while (true) {
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
            if (poller.pollin(2)) {
                byte cmd = commandSocket.recvCommand();
                if (cmd == CommandSocket.STOP) {
                    break;
                }
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
        //XXX: For now force closing sockets to prevent hang on shutdown
        frontend.setLinger(0);
        backend.setLinger(0);
        frontend.close();
        backend.close();
    }

    @Override
    protected void triggerShutdown() {
        commandSocket.sendCommand(CommandSocket.STOP);
    }

    public TTransport getClient() {
        return clientFactory.create();
    }

}
