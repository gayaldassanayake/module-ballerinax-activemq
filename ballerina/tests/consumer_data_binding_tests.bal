// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/test;

// TC-DATABIND-01: a string payload sent and received as `string` round-trips via TextMessage
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingString() returns error? {
    check drainQueue("it.databind.string.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    string payload = "This is a sample payload";
    check producer->send({payload}, {queueName: "it.databind.string.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.string.queue"});
    record {|*Message; string payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "string payload should round-trip");
    }
}

// TC-DATABIND-02: a record payload round-trips via MapMessage
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingRecord() returns error? {
    check drainQueue("it.databind.record.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    record {|string name; int age;|} payload = {name: "Ana", age: 30};
    check producer->send({payload}, {queueName: "it.databind.record.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.record.queue"});
    record {|*Message; record {|string name; int age;|} payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "record payload should round-trip via MapMessage");
    }
}

// TC-DATABIND-03: byte[] payload behavior is unchanged when explicitly requested
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingByteArray() returns error? {
    check drainQueue("it.databind.bytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    byte[] payload = [72, 101, 108, 108, 111]; // "Hello"
    check producer->send({payload}, {queueName: "it.databind.bytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.bytes.queue"});
    record {|*Message; byte[] payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload, payload, "byte array payload should round-trip byte-for-byte");
    }
}

// TC-DATABIND-04: leaving T at its default (Message) falls back to the JMS-message-appropriate
// representation, matching pre-task-20 behavior for a BytesMessage.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingUntypedDefault() returns error? {
    check drainQueue("it.databind.untyped.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "Hello".toBytes()}, {queueName: "it.databind.untyped.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.untyped.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertTrue(received.payload is byte[],
            "untyped receive should fall back to raw bytes for a BytesMessage");
    }
}

// TC-DATABIND-05: requesting a type that doesn't fit the actual message returns an activemq:Error
// rather than panicking, with a clear "Data binding failed" message.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingTypeMismatch() returns error? {
    check drainQueue("it.databind.mismatch.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: "not a number"}, {queueName: "it.databind.mismatch.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.mismatch.queue"});
    record {|*Message; int payload;|}|error? received = consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is error, "should return an error for a type mismatch, not panic");
    if received is error {
        test:assertEquals(received.message(),
            "Data binding failed: Cannot bind TextMessage to type 'int'. Expected 'string' or 'xml'");
    }
}

// TC-DATABIND-06: a per-call messageSelector overrides the consumer's own configured selector,
// via a temporary internal consumer scoped to just that call.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingMessageSelectorOverride() returns error? {
    check drainQueue("it.databind.selector.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({
        payload: "APAC order".toBytes(),
        properties: {"region": "APAC"}
    }, {queueName: "it.databind.selector.queue"});
    check producer->send({
        payload: "EMEA order".toBytes(),
        properties: {"region": "EMEA"}
    }, {queueName: "it.databind.selector.queue"});
    check producer->close();

    // Configured with a selector matching neither message, so the persistent consumer never
    // competes with the per-call selector overrides below for either message.
    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.databind.selector.queue"}, messageSelector = "region = 'UNUSED'");

    record {|*Message; byte[] payload;|}? apacMsg =
        check consumer->receive(5000, messageSelector = "region = 'APAC'");
    test:assertTrue(apacMsg is Message, "should receive the APAC message via the per-call selector override");
    if apacMsg is Message {
        test:assertEquals(check string:fromBytes(apacMsg.payload), "APAC order");
    }

    record {|*Message; byte[] payload;|}? emeaMsg =
        check consumer->receive(3000, messageSelector = "region = 'EMEA'");
    check consumer->close();
    test:assertTrue(emeaMsg is Message,
        "should receive the EMEA message via a second, different per-call selector override");
    if emeaMsg is Message {
        test:assertEquals(check string:fromBytes(emeaMsg.payload), "EMEA order");
    }
}
