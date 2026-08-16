// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/lang.runtime;
import ballerina/test;
import ballerina/time;

// Dedicated listener for client integration tests that need a subscriber (topic tests).
listener Listener clientTestListener = check new Listener(BROKER_URL);

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Basic send and receive on a queue
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientSendAndReceiveFromQueue() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "basic-1",
        payload: "Hello ActiveMQ".toBytes()
    }, {queueName: "client.test.basic.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.test.basic.queue"});
    record {|*Message; byte[] payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        string content = check string:fromBytes(received.payload);
        test:assertEquals(content, "Hello ActiveMQ", "payload content should match");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: receive returns () when queue is empty within timeout
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientReceiveReturnsNilOnTimeout() returns error? {
    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.test.empty.queue"});
    Message? received = check consumer->receive(1000);
    check consumer->close();
    test:assertTrue(received is (), "should return nil when no message arrives in timeout");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Multiple messages are received in the order they were sent
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientMultipleMessages() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    string[] payloads = ["First", "Second", "Third"];
    foreach string p in payloads {
        check producer->send({
            messageId: p,
            payload: p.toBytes()
        }, {queueName: "client.test.multi.queue"});
    }
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.test.multi.queue"});
    string[] received = [];
    record {|*Message; byte[] payload;|}? msg = check consumer->receive(3000);
    while msg is Message {
        received.push(check string:fromBytes(msg.payload));
        msg = check consumer->receive(2000);
    }
    check consumer->close();
    test:assertEquals(received.length(), 3, "should receive all 3 sent messages");
    test:assertEquals(received, payloads, "messages should arrive in send order");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: JMS headers and custom properties survive the send/receive roundtrip
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientMessageFieldsRoundtrip() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "fields-1",
        payload: "Roundtrip payload".toBytes(),
        correlationId: "corr-abc-123",
        'type: "TestOrder",
        persistent: true,
        priority: 7,
        properties: {
            "category": "electronics",
            "region": "APAC"
        }
    }, {queueName: "client.test.fields.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.test.fields.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive message");
    if received is Message {
        test:assertEquals(received.correlationId, "corr-abc-123", "correlationId should roundtrip");
        test:assertEquals(received.'type, "TestOrder", "type should roundtrip");
        boolean? persistent = received.persistent;
        test:assertTrue(persistent is boolean && persistent == true, "persistent should be true");
        int? priority = received.priority;
        test:assertTrue(priority is int && priority >= 7, "priority should be preserved");
        map<Property>? props = received.properties;
        test:assertTrue(props is map<Property>, "custom properties should be present");
        if props is map<Property> {
            test:assertEquals(props["category"], "electronics");
            test:assertEquals(props["region"], "APAC");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4b: an explicit priority of 0 (the legal JMS minimum) round-trips as 0,
// not absent — it must not be dropped the way expiry: 0 used to be.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientPriorityZeroRoundtrips() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "priority-zero-1",
        payload: "priority zero payload".toBytes(),
        priority: 0
    }, {queueName: "client.test.priorityzero.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.test.priorityzero.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive message");
    if received is Message {
        test:assertEquals(received.priority, 0, "priority 0 should round-trip, not be dropped as absent");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Persistent and non-persistent delivery modes are reflected in received
//         messages
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientPersistenceField() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "p-1",
        payload: "persistent".toBytes(),
        persistent: true
    }, {queueName: "client.test.persist.queue"});
    check producer->send({
        messageId: "np-1",
        payload: "non-persistent".toBytes(),
        persistent: false
    }, {queueName: "client.test.nonpersist.queue"});
    check producer->close();

    MessageConsumer persistConsumer = check new (BROKER_URL, destination = {queueName: "client.test.persist.queue"});
    Message? pMsg = check persistConsumer->receive(3000);
    check persistConsumer->close();

    MessageConsumer nonpersistConsumer = check new (BROKER_URL,
        destination = {queueName: "client.test.nonpersist.queue"});
    Message? npMsg = check nonpersistConsumer->receive(3000);
    check nonpersistConsumer->close();

    test:assertTrue(pMsg is Message, "persistent message should be received");
    test:assertTrue(npMsg is Message, "non-persistent message should be received");
    if pMsg is Message {
        test:assertTrue(pMsg.persistent == true, "persistent field should be true");
    }
    if npMsg is Message {
        test:assertTrue(npMsg.persistent == false, "persistent field should be false");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6: replyTo header is preserved and returned as a typed destination
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientReplyToField() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "rr-1",
        payload: "Request".toBytes(),
        replyTo: {queueName: "client.test.reply.queue"}
    }, {queueName: "client.test.replyto.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.test.replyto.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive message with replyTo set");
    if received is Message {
        Destination? replyTo = received.replyTo;
        test:assertTrue(replyTo is Queue, "replyTo should be a queue destination");
        if replyTo is Queue {
            test:assertEquals(replyTo.queueName, "client.test.reply.queue");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7: Producer can publish to a JMS topic; a Listener service receives it
// ─────────────────────────────────────────────────────────────────────────────

isolated int clientTopicReceivedCount = 0;

@test:Config {
    groups: ["client", "topics"]
}
isolated function testClientSendToTopic() returns error? {
    lock { clientTopicReceivedCount = 0; }
    Service topicSvc = @ServiceConfig {
        topicName: "client.test.topic"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                clientTopicReceivedCount += 1;
            }
        }
    };
    check clientTestListener.attach(topicSvc, "client-topic-svc");
    // Allow the subscriber to fully register before the producer sends.
    runtime:sleep(2);

    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "topic-msg-1",
        payload: "Topic message from producer".toBytes()
    }, {topicName: "client.test.topic"});
    check producer->close();

    runtime:sleep(2);
    lock {
        test:assertEquals(clientTopicReceivedCount, 1,
            "listener service should receive the message published by the producer to a topic");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 8: Calling send after close() returns an Error
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientClose() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->close();
    Error? result = producer->send({
        messageId: "after-close",
        payload: "should fail".toBytes()
    }, {queueName: "client.test.close.queue"});
    test:assertTrue(result is Error, "send after close should return an Error");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 9: messageId, timestamp, and destination are populated by the broker
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientBrokerPopulatedFields() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "sent-id",
        payload: "Broker fields test".toBytes()
    }, {queueName: "client.test.broker.fields.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL,
        destination = {queueName: "client.test.broker.fields.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive message");
    if received is Message {
        // Broker assigns its own message ID
        test:assertTrue(received.messageId is string, "broker-assigned messageId should be present");
        test:assertTrue((<string>received.messageId).length() > 0, "broker-assigned messageId should be non-empty");
        // Broker sets the timestamp at send time
        int? timestamp = received.timestamp;
        test:assertTrue(timestamp is int && timestamp > 0, "broker-assigned timestamp should be > 0");
        // Destination should reflect where the message landed
        Destination? destination = received.destination;
        test:assertTrue(destination is Queue, "destination should be a queue destination");
        if destination is Queue {
            test:assertEquals(destination.queueName, "client.test.broker.fields.queue");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 10: receive with a message selector — only matching messages returned
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "selector"]
}
isolated function testClientReceiveWithSelector() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    // Send two messages: one with region=APAC and one with region=EMEA.
    check producer->send({
        messageId: "sel-apac",
        payload: "APAC order".toBytes(),
        properties: {"region": "APAC"}
    }, {queueName: "client.test.selector.queue"});
    check producer->send({
        messageId: "sel-emea",
        payload: "EMEA order".toBytes(),
        properties: {"region": "EMEA"}
    }, {queueName: "client.test.selector.queue"});
    check producer->close();

    // Receive with selector — should get only APAC even though EMEA arrived first.
    MessageConsumer apacConsumer = check new (BROKER_URL,
        destination = {queueName: "client.test.selector.queue"}, messageSelector = "region = 'APAC'");
    record {|*Message; byte[] payload;|}? apacMsg = check apacConsumer->receive(5000);
    check apacConsumer->close();

    // Drain the EMEA message that was skipped.
    MessageConsumer emeaConsumer = check new (BROKER_URL,
        destination = {queueName: "client.test.selector.queue"}, messageSelector = "region = 'EMEA'");
    Message? emeaMsg = check emeaConsumer->receive(3000);
    check emeaConsumer->close();

    test:assertTrue(apacMsg is Message, "should receive the APAC message via selector");
    if apacMsg is Message {
        string content = check string:fromBytes(apacMsg.payload);
        test:assertEquals(content, "APAC order", "selector should return only the matching message");
    }
    test:assertTrue(emeaMsg is Message, "EMEA message should also be receivable with its selector");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 11: receive without a configured selector still works (backward compat)
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "selector"]
}
isolated function testClientReceiveWithoutSelector() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        messageId: "no-sel-1",
        payload: "no selector".toBytes()
    }, {queueName: "client.test.no.selector.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL,
        destination = {queueName: "client.test.no.selector.queue"});
    Message? msg = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(msg is Message, "receive without a configured selector should still work");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 12: Transacted producer — commit delivers all messages atomically
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionCommit() returns error? {
    MessageProducer producer = check new (BROKER_URL, transacted = true);
    check producer->send({
        messageId: "tx-1",
        payload: "tx message 1".toBytes()
    }, {queueName: "client.tx.commit.queue"});
    check producer->send({
        messageId: "tx-2",
        payload: "tx message 2".toBytes()
    }, {queueName: "client.tx.commit.queue"});
    check producer->'commit();
    check producer->close();

    // Both messages must now be visible.
    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.tx.commit.queue"});
    Message? msg1 = check consumer->receive(5000);
    Message? msg2 = check consumer->receive(5000);
    check consumer->close();

    test:assertTrue(msg1 is Message, "first committed message should be received");
    test:assertTrue(msg2 is Message, "second committed message should be received");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 13: Transacted producer — rollback discards all messages
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionRollback() returns error? {
    check drainQueue("client.tx.rollback.queue");
    MessageProducer producer = check new (BROKER_URL, transacted = true);
    check producer->send({
        messageId: "tx-rb-1",
        payload: "will be discarded".toBytes()
    }, {queueName: "client.tx.rollback.queue"});
    check producer->'rollback();
    check producer->close();

    // Queue must be empty — rollback discarded the message.
    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.tx.rollback.queue"});
    Message? msg = check consumer->receive(2000);
    check consumer->close();
    test:assertTrue(msg is (), "rolled-back message must not be delivered");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 14: Transacted producer — close without commit implicitly rolls back
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionCloseRollsBack() returns error? {
    check drainQueue("client.tx.close.queue");
    MessageProducer producer = check new (BROKER_URL, transacted = true);
    check producer->send({
        messageId: "tx-close-1",
        payload: "implicit rollback".toBytes()
    }, {queueName: "client.tx.close.queue"});
    check producer->close(); // close without commit — broker must discard the message

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.tx.close.queue"});
    Message? msg = check consumer->receive(2000);
    check consumer->close();
    test:assertTrue(msg is (), "closing a transaction without committing must roll back");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 15: Scheduled delivery — the message is withheld until the scheduled delay elapses
// (requires schedulerSupport="true" in the test broker's activemq.xml)
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "scheduler"]
}
function testClientScheduledDelivery() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    // Schedule delivery 4 seconds in the future.
    check producer->send({
        messageId: "sched-1",
        payload: "scheduled message".toBytes(),
        scheduledDelay: 4000
    }, {queueName: "client.scheduled.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: "client.scheduled.queue"});
    // Immediate receive must time out — message is not yet due.
    Message? early = check consumer->receive(1000);
    test:assertTrue(early is (), "message should not be delivered before the scheduled delay");

    // Wait for the scheduler to release the message.
    runtime:sleep(6);
    Message? msg = check consumer->receive(3000);
    check consumer->close();
    test:assertTrue(msg is Message, "message should be delivered after the scheduled delay");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 16: Every `activemq:Property` value type (boolean, int, byte, float, string)
//          survives the send/receive roundtrip with the correct Ballerina type
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client"]
}
isolated function testClientPropertyTypesRoundtrip() returns error? {
    MessageProducer producer = check new (BROKER_URL);
    byte byteProp = 200;
    check producer->send({
        payload: "property types".toBytes(),
        properties: {
            "strProp": "hello",
            "intProp": 42,
            "boolProp": true,
            "floatProp": 3.5,
            "byteProp": byteProp
        }
    }, {queueName: "client.test.propertytypes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL,
        destination = {queueName: "client.test.propertytypes.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the message");
    if received is Message {
        map<Property>? props = received.properties;
        test:assertTrue(props is map<Property>, "properties should be present");
        if props is map<Property> {
            test:assertEquals(props["strProp"], "hello");
            test:assertEquals(props["intProp"], 42);
            test:assertEquals(props["boolProp"], true);
            test:assertEquals(props["floatProp"], 3.5);
            Property? roundtrippedByte = props["byteProp"];
            test:assertTrue(roundtrippedByte is byte, "byteProp should roundtrip as a byte");
            if roundtrippedByte is byte {
                test:assertEquals(roundtrippedByte, byteProp);
            }
        }
    }
}

// TC-PROPS-01: a byte[] property isn't a legal JMS message property - send() must drop it gracefully, not crash.
@test:Config {
    groups: ["client"]
}
isolated function testClientSendWithUnsupportedPropertyTypeDoesNotFail() returns error? {
    check drainQueue("client.test.unsupportedprop.queue");
    MessageProducer producer = check new (BROKER_URL);
    check producer->send({
        payload: "unsupported property".toBytes(),
        properties: {
            "strProp": "hello",
            "bytesProp": "unsupported".toBytes()
        }
    }, {queueName: "client.test.unsupportedprop.queue"});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL,
        destination = {queueName: "client.test.unsupportedprop.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should still receive the message");
    if received is Message {
        map<Property>? props = received.properties;
        test:assertTrue(props is map<Property>, "the other, supported property should still be present");
        if props is map<Property> {
            test:assertEquals(props["strProp"], "hello");
            test:assertFalse(props.hasKey("bytesProp"), "the unsupported byte[] property should be dropped");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 17: Concurrent send() calls on one MessageProducer are serialized safely.
// MessageProducer now holds one persistent JMS Session for its whole lifetime
// (replacing the old Client's per-call session), so this exercises the fix for
// the JMS Session thread-safety requirement under concurrent isolated-client calls.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "concurrency"]
}
function testConcurrentProducerSendIsThreadSafe() returns error? {
    string queueName = "client.test.concurrency.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL);

    future<Error?> f1 = start producer->send({messageId: "conc-1", payload: "one".toBytes()},
            {queueName: queueName});
    future<Error?> f2 = start producer->send({messageId: "conc-2", payload: "two".toBytes()},
            {queueName: queueName});
    future<Error?> f3 = start producer->send({messageId: "conc-3", payload: "three".toBytes()},
            {queueName: queueName});

    Error? r1 = wait f1;
    Error? r2 = wait f2;
    Error? r3 = wait f3;
    check producer->close();

    test:assertTrue(r1 is (), "concurrent send 1 should not error");
    test:assertTrue(r2 is (), "concurrent send 2 should not error");
    test:assertTrue(r3 is (), "concurrent send 3 should not error");

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: queueName});
    int received = 0;
    Message? msg = check consumer->receive(3000);
    while msg is Message {
        received += 1;
        msg = check consumer->receive(1000);
    }
    check consumer->close();
    test:assertEquals(received, 3,
        "all 3 concurrently-sent messages should arrive intact, none lost or corrupted");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 17b: close() must not hang behind a concurrent, indefinitely-blocking
// receive(0) on the same consumer.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "concurrency"]
}
function testReceiveDoesNotDeadlockClose() returns error? {
    string queueName = "client.test.receive.deadlock.queue";
    check drainQueue(queueName);
    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName: queueName});

    future<Message|Error?> f = start consumer->receive(0, Message);
    runtime:sleep(1);

    Error? closeResult = consumer->close();
    test:assertTrue(closeResult is (), "close() must return promptly, not hang behind a blocking receive(0)");

    Message|Error? received = wait f;
    test:assertTrue(received is (), "a receive() blocked when the consumer closes should unblock with nil");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 18: A transacted producer's commit()/rollback() after close() returns an
// Error instead of panicking.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionCommitAfterCloseReturnsError() returns error? {
    MessageProducer producer = check new (BROKER_URL, transacted = true);
    check producer->close();
    Error? result = producer->'commit();
    test:assertTrue(result is Error, "commit after close should return an Error, not panic");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 19: A transacted producer's close() is idempotent, same as the plain
// producer's (cleanup_tests.bal TC-CLEANUP-01), verified for the transacted
// path specifically since task 6 collapsed Transaction into both.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientTransactionDoubleCloseIdempotent() returns error? {
    MessageProducer producer = check new (BROKER_URL, transacted = true);
    check producer->close();
    // Second close: () or Error are both acceptable; what matters is no panic.
    do { check producer->close(); } on fail { }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 20: A client-side send failure mid-transaction (an unsupported payload
// type never reaches the broker) doesn't poison the transacted session --
// rollback and close still work cleanly afterward, and nothing is delivered,
// including the earlier message that was mapped successfully but never
// committed.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
function testClientTransactionRollbackAfterSendFailure() returns error? {
    string queueName = "client.tx.sendfailure.queue";
    check drainQueue(queueName);
    MessageProducer producer = check new (BROKER_URL, transacted = true);
    check producer->send({
        messageId: "tx-fail-1",
        payload: "will never be committed".toBytes()
    }, {queueName});

    // An out-of-range priority fails validation before ever reaching the broker.
    Error? result = producer->send({payload: "will fail".toBytes(), priority: 15}, {queueName});
    test:assertTrue(result is Error, "sending with an invalid priority should fail, not panic");

    check producer->'rollback();
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, destination = {queueName});
    Message? msg = check consumer->receive(2000);
    check consumer->close();
    test:assertTrue(msg is (),
        "rollback after a send failure must discard the whole transaction, including the earlier message");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 21: A transacted consumer's commit()/rollback() after close() returns an
// Error instead of panicking (symmetric with test 18, for MessageConsumer).
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientConsumerTransactionCommitAfterCloseReturnsError() returns error? {
    MessageConsumer consumer = check new (BROKER_URL,
        ackMode = SESSION_TRANSACTED, destination = {queueName: "client.tx.consumer.close.queue"});
    check consumer->close();
    Error? result = consumer->'commit();
    test:assertTrue(result is Error, "commit after close should return an Error, not panic");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 22: A transacted consumer's close() is idempotent (symmetric with test
// 19, for MessageConsumer).
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "transaction"]
}
isolated function testClientConsumerTransactionDoubleCloseIdempotent() returns error? {
    MessageConsumer consumer = check new (BROKER_URL,
        ackMode = SESSION_TRANSACTED, destination = {queueName: "client.tx.consumer.doubleclose.queue"});
    check consumer->close();
    // Second close: () or Error are both acceptable; what matters is no panic.
    do { check consumer->close(); } on fail { }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 23: commit() must not run concurrently with an in-flight receive() on the
// same session - a JMS Session is single-threaded, so commit() has to wait for a
// blocked receive() to release the session before it can proceed.
// ─────────────────────────────────────────────────────────────────────────────

@test:Config {
    groups: ["client", "concurrency"]
}
function testCommitWaitsForInFlightReceive() returns error? {
    string queueName = "client.session.serialize.queue";
    check drainQueue(queueName);
    MessageConsumer consumer = check new (BROKER_URL,
        ackMode = SESSION_TRANSACTED, destination = {queueName});

    future<Message|Error?> f = start consumer->receive(2000, Message);
    runtime:sleep(0.2); // let the receive() strand grab the session lock first

    time:Utc before = time:utcNow();
    check consumer->'commit();
    time:Utc after = time:utcNow();

    Message|Error? received = wait f;
    test:assertTrue(received is (), "empty queue receive should time out with nil");

    decimal elapsedSeconds = time:utcDiffSeconds(after, before);
    test:assertTrue(elapsedSeconds > 1d,
        "commit() must wait for the in-flight receive() to release the session, not run concurrently with it");

    check consumer->close();
}
