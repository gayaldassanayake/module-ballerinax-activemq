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

package io.ballerina.lib.activemq.consumer;

import io.ballerina.lib.activemq.util.ActiveMQDatabindingException;
import io.ballerina.lib.activemq.util.ConnectionFactoryUtils;
import io.ballerina.lib.activemq.util.MessageMapper;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

import static io.ballerina.lib.activemq.util.ActiveMQConstants.ACTIVEMQ_ERROR;
import static io.ballerina.lib.activemq.util.CommonUtils.createError;
import static io.ballerina.lib.activemq.util.CommonUtils.getAcknowledgementMode;

/**
 * Native implementation of the Ballerina ActiveMQ MessageConsumer. Holds one persistent JMS
 * session and consumer for the instance's whole lifetime; every method that touches them is
 * synchronized on the shared {@link ConsumerState}, since a JMS {@code Session} is not
 * thread-safe and Ballerina's {@code isolated client class} permits concurrent remote calls.
 *
 * @since 0.1.0
 */
public final class Actions {

    private Actions() {
    }

    static final String NATIVE_STATE = "native.state";

    private static final class ConsumerState {
        final Connection connection;
        final Session session;
        final MessageConsumer consumer;
        final Destination destination;
        final boolean transacted;
        volatile boolean closed = false;

        ConsumerState(Connection connection, Session session, MessageConsumer consumer, Destination destination,
                boolean transacted) {
            this.connection = connection;
            this.session = session;
            this.consumer = consumer;
            this.destination = destination;
            this.transacted = transacted;
        }
    }

    public static Object init(BObject bConsumer, BString url, BMap<BString, Object> configurations) {
        try {
            ConsumerConfig config = new ConsumerConfig(configurations);
            ActiveMQConnectionFactory factory =
                    ConnectionFactoryUtils.createConnectionFactory(url.getValue(), config.connectionConfig());
            Connection connection = factory.createConnection();
            connection.start();
            int sessionMode = getAcknowledgementMode(config.ackMode());
            boolean transacted = sessionMode == Session.SESSION_TRANSACTED;
            Session session = connection.createSession(sessionMode);
            Destination destination = MessageMapper.toJmsDestination(session, config.destination());
            MessageConsumer consumer = config.messageSelector() != null
                    ? session.createConsumer(destination, config.messageSelector())
                    : session.createConsumer(destination);
            ConsumerState state = new ConsumerState(connection, session, consumer, destination, transacted);
            bConsumer.addNativeData(NATIVE_STATE, state);
        } catch (Exception e) {
            return createError(ACTIVEMQ_ERROR, "Failed to initialize consumer: " + e.getMessage(), e);
        }
        return null;
    }

    public static Object receive(BObject bConsumer, long timeoutMs, Object messageSelector, BTypedesc bTypedesc) {
        return execute(bConsumer, "receive message", state -> {
            Message jmsMsg = receiveWithOptionalSelector(state, messageSelector, timeoutMs, false);
            return jmsMsg == null ? null : MessageMapper.toBallerinaMessage(jmsMsg, bTypedesc);
        });
    }

    public static Object receiveNoWait(BObject bConsumer, Object messageSelector, BTypedesc bTypedesc) {
        return execute(bConsumer, "receive message", state -> {
            Message jmsMsg = receiveWithOptionalSelector(state, messageSelector, 0, true);
            return jmsMsg == null ? null : MessageMapper.toBallerinaMessage(jmsMsg, bTypedesc);
        });
    }

    /**
     * Receives using the persistent consumer, unless {@code messageSelector} overrides it for this
     * call — in which case a temporary consumer scoped to that selector is created, used once, and
     * closed. Both paths run inside {@code execute}'s {@code synchronized (state)} block, so this
     * never races with other operations on the same session.
     */
    private static Message receiveWithOptionalSelector(ConsumerState state, Object messageSelector,
            long timeoutMs, boolean noWait) throws JMSException {
        if (!(messageSelector instanceof BString bSelector) || bSelector.getValue().isEmpty()) {
            return noWait ? state.consumer.receiveNoWait() : state.consumer.receive(timeoutMs);
        }
        MessageConsumer temp = state.session.createConsumer(state.destination, bSelector.getValue());
        try {
            return noWait ? temp.receiveNoWait() : temp.receive(timeoutMs);
        } finally {
            temp.close();
        }
    }

    /**
     * Acknowledges a previously received message. Operates directly on the native JMS message
     * stashed on the record by {@link MessageMapper#toBallerinaMessage(Message)}, mirroring the
     * existing listener-side {@code Caller.acknowledge} - no consumer instance reference needed,
     * since JMS's {@code Message.acknowledge()} acknowledges everything up to that message within
     * its own session regardless of which message instance it's called on.
     */
    public static Object acknowledge(BMap<BString, Object> message) {
        try {
            Object nativeMessage = message.getNativeData(MessageMapper.NATIVE_MESSAGE);
            if (nativeMessage instanceof Message jmsMsg) {
                jmsMsg.acknowledge();
            }
        } catch (JMSException e) {
            return createError(ACTIVEMQ_ERROR, "Failed to acknowledge message: " + e.getMessage(), e);
        }
        return null;
    }

    public static Object commit(BObject bConsumer) {
        return execute(bConsumer, "commit transaction", state -> {
            if (!state.transacted) {
                throw new JMSException(
                        "'commit is only valid when the consumer is configured with ackMode: SESSION_TRANSACTED");
            }
            state.session.commit();
            return null;
        });
    }

    public static Object rollback(BObject bConsumer) {
        return execute(bConsumer, "rollback transaction", state -> {
            if (!state.transacted) {
                throw new JMSException(
                        "'rollback is only valid when the consumer is configured with ackMode: SESSION_TRANSACTED");
            }
            state.session.rollback();
            return null;
        });
    }

    public static Object close(BObject bConsumer) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return null;
        }
        synchronized (state) {
            if (state.closed) {
                return null; // idempotent
            }
            state.closed = true;
            try {
                state.connection.stop();
            } catch (JMSException ignored) {
                // stop() failure is non-fatal; proceed to close() regardless
            }
            try {
                state.connection.close();
            } catch (JMSException e) {
                return createError(ACTIVEMQ_ERROR, "Failed to close consumer: " + e.getMessage(), e);
            }
        }
        return null;
    }

    @FunctionalInterface
    private interface JmsAction {
        Object run(ConsumerState state) throws JMSException;
    }

    private static Object execute(BObject bConsumer, String operation, JmsAction action) {
        ConsumerState state = (ConsumerState) bConsumer.getNativeData(NATIVE_STATE);
        if (state == null) {
            return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is not initialized");
        }
        synchronized (state) {
            if (state.closed) {
                return createError(ACTIVEMQ_ERROR, "ActiveMQ consumer is already closed");
            }
            try {
                return action.run(state);
            } catch (JMSException e) {
                return createError(ACTIVEMQ_ERROR, String.format("Failed to %s: %s", operation, e.getMessage()), e);
            } catch (ActiveMQDatabindingException e) {
                return createError(ACTIVEMQ_ERROR, e.getMessage(), e);
            }
        }
    }
}
