/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.activemq.producer;

import io.ballerina.lib.activemq.util.CommonUtils;
import io.ballerina.lib.activemq.util.ConnectionFactoryUtils;
import io.ballerina.lib.activemq.util.MessageMapper;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;

/**
 * Native implementation of the Ballerina ActiveMQ MessageProducer. Holds one persistent JMS
 * session and producer for the instance's whole lifetime; every method that touches them is
 * synchronized on the shared {@link ProducerState}, since a JMS {@code Session} is not
 * thread-safe and Ballerina's {@code isolated client class} permits concurrent remote calls.
 *
 * @since 0.1.0
 */
public final class Actions {

    private Actions() {
    }

    static final String NATIVE_STATE = "native.state";

    private static final class ProducerState {
        final Connection connection;
        final Session session;
        final MessageProducer producer;
        final boolean transacted;
        final BMap<BString, Object> defaultDestination;
        volatile boolean closed = false;

        ProducerState(Connection connection, Session session, MessageProducer producer, boolean transacted,
                      BMap<BString, Object> defaultDestination) {
            this.connection = connection;
            this.session = session;
            this.producer = producer;
            this.transacted = transacted;
            this.defaultDestination = defaultDestination;
        }
    }

    public static Object init(BObject bProducer, BString url, BMap<BString, Object> configurations) {
        Connection connection = null;
        Session session = null;
        try {
            ProducerConfig config = new ProducerConfig(configurations);
            ActiveMQConnectionFactory factory =
                    ConnectionFactoryUtils.createConnectionFactory(url.getValue(), config.connectionConfig());
            connection = factory.createConnection();
            connection.start();
            int sessionMode = config.transacted() ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE;
            session = connection.createSession(sessionMode);
            MessageProducer producer = session.createProducer(null); // unidentified producer
            ProducerState state = new ProducerState(
                    connection, session, producer, config.transacted(), config.destination());
            bProducer.addNativeData(NATIVE_STATE, state);
        } catch (Exception e) {
            cleanupOnInitFailure(connection, session);
            return createError(ACTIVEMQ_ERROR, "Failed to initialize producer: " + e.getMessage(), e);
        }
        return null;
    }

    private static void cleanupOnInitFailure(Connection connection, Session session) {
        if (session != null) {
            CommonUtils.closeQuietly(session::close);
        }
        if (connection != null) {
            CommonUtils.closeQuietly(connection::close);
        }
    }

    public static Object send(BObject bProducer, BMap<BString, Object> bMessage, Object destinationObj) {
        return execute(bProducer, "send message", state -> {
            BMap<BString, Object> destination = destinationObj instanceof BMap<?, ?> rawDest
                    ? castToDestinationMap(rawDest) : state.defaultDestination;
            if (destination == null) {
                throw new JMSException(
                        "No destination specified for this send call, and no default destination configured");
            }
            Destination dest = MessageMapper.toJmsDestination(state.session, destination);
            Message jmsMsg = MessageMapper.toJmsMessage(state.session, bMessage);
            state.producer.send(dest, jmsMsg, MessageMapper.getDeliveryMode(bMessage),
                    MessageMapper.getPriority(bMessage), MessageMapper.getTTL(bMessage));
            return null;
        });
    }

    public static Object commit(BObject bProducer) {
        return execute(bProducer, "commit transaction", state -> {
            if (!state.transacted) {
                throw new JMSException(
                        "'commit is only valid when the producer is configured with transacted: true");
            }
            state.session.commit();
            return null;
        });
    }

    public static Object rollback(BObject bProducer) {
        return execute(bProducer, "rollback transaction", state -> {
            if (!state.transacted) {
                throw new JMSException(
                        "'rollback is only valid when the producer is configured with transacted: true");
            }
            state.session.rollback();
            return null;
        });
    }

    public static Object close(BObject bProducer) {
        ProducerState state = (ProducerState) bProducer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return null;
        }
        synchronized (state) {
            if (state.closed) {
                return null; // idempotent
            }
            try {
                state.connection.stop();
            } catch (JMSException ignored) {
                // stop() failure is non-fatal; proceed to close() regardless
            }
            try {
                state.connection.close();
            } catch (JMSException e) {
                return createError(ACTIVEMQ_ERROR, "Failed to close producer: " + e.getMessage(), e);
            }
            state.closed = true;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static BMap<BString, Object> castToDestinationMap(BMap<?, ?> rawDest) {
        return (BMap<BString, Object>) rawDest;
    }

    @FunctionalInterface
    private interface JmsAction {
        Object run(ProducerState state) throws JMSException;
    }

    private static Object execute(BObject bProducer, String operation, JmsAction action) {
        ProducerState state = (ProducerState) bProducer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ producer is not initialized");
        }
        synchronized (state) {
            if (state.closed) {
                return createError(ACTIVEMQ_ERROR, "ActiveMQ producer is already closed");
            }
            try {
                return action.run(state);
            } catch (JMSException e) {
                return createError(ACTIVEMQ_ERROR, String.format("Failed to %s: %s", operation, e.getMessage()), e);
            }
        }
    }
}
