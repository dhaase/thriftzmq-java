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

import org.jeromq.ZMQ;

/**
 *
 * @author Vyacheslav Baranov
 */
public class TZMQTransportFactory {

    private final ZMQ.Context context;
    private final String address;
    private final int socketType;
    private final boolean bind;

    public TZMQTransportFactory(ZMQ.Context context, String address, int socketType, boolean bind) {
        this.context = context;
        this.address = address;
        this.socketType = socketType;
        this.bind = bind;
    }

    public ZMQ.Context getContext() {
        return context;
    }

    public String getAddress() {
        return address;
    }

    public TZMQTransport create() {
        return new TZMQTransport(context, address, socketType, bind);
    }

}
