/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.thriftzmq;

import org.apache.thrift.transport.TTransport;
import org.jeromq.ZMQ;

/**
 *
 * @author wildfire
 */
public class TZMQClientFactory {

    public static TTransport create(ZMQ.Context context, String address) {
        return new TransportSocket(context, address, ZMQ.REQ, false);
    }
}
