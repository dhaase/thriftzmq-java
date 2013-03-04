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

import com.google.common.util.concurrent.Service;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jeromq.ZMQ;

/**
 *
 * @author Vyacheslav Baranov
 */
public abstract class TZMQServer implements Service {

    public static abstract class AbstractServerArgs<T extends AbstractServerArgs<T>> {

        protected final ZMQ.Context context;
        protected final String address;
        protected TProcessorFactory processorFactory;
        protected TProtocolFactory inputProtocolFactory = new TBinaryProtocol.Factory();
        protected TProtocolFactory outputProtocolFactory = new TBinaryProtocol.Factory();

        public AbstractServerArgs(ZMQ.Context context, String address) {
            this.context = context;
            this.address = address;
        }

        @SuppressWarnings("unchecked")
        public T processorFactory(TProcessorFactory factory) {
            this.processorFactory = factory;
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T processor(TProcessor processor) {
            this.processorFactory = new TProcessorFactory(processor);
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T protocolFactory(TProtocolFactory factory) {
            this.inputProtocolFactory = factory;
            this.outputProtocolFactory = factory;
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T inputProtocolFactory(TProtocolFactory factory) {
            this.inputProtocolFactory = factory;
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T outputProtocolFactory(TProtocolFactory factory) {
            this.outputProtocolFactory = factory;
            return (T) this;
        }
    }

    protected final ZMQ.Context context;
    protected final String address;
    protected final TProcessorFactory processorFactory;
    protected final TProtocolFactory inputProtocolFactory;
    protected final TProtocolFactory outputProtocolFactory;

    public TZMQServer(AbstractServerArgs<?> args) {
        this.context = args.context;
        this.address = args.address;
        this.processorFactory = args.processorFactory;
        this.inputProtocolFactory = args.inputProtocolFactory;
        this.outputProtocolFactory = args.outputProtocolFactory;
    }

}
