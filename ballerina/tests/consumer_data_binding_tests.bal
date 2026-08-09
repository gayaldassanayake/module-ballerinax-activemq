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

// TC-DATABIND-01b: an untyped (unnarrowed) receive() on a TextMessage returns byte[], not string.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingUntypedTextMessageIsBytes() returns error? {
    check drainQueue("it.databind.untypedtext.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    string payload = "This is a sample payload";
    check producer->send({payload}, {queueName: "it.databind.untypedtext.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.untypedtext.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertTrue(received.payload is byte[],
            "untyped receive() on a TextMessage should return byte[], not string");
        test:assertEquals(check string:fromBytes(<byte[]>received.payload), payload);
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

// TC-DATABIND-02b: a record payload with a byte[] field round-trips via MapMessage, narrowed.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingRecordWithByteArrayField() returns error? {
    check drainQueue("it.databind.mapbytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    record {|string name; byte[] data;|} payload = {name: "blob", data: "hello".toBytes()};
    check producer->send({payload}, {queueName: "it.databind.mapbytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.databind.mapbytes.queue"});
    record {|*Message; record {|string name; byte[] data;|} payload;|}? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        test:assertEquals(received.payload.name, payload.name);
        test:assertEquals(received.payload.data, payload.data,
            "a byte[] MapMessage entry should round-trip instead of being dropped");
    }
}

// TC-DATABIND-02c: an untyped map<Property> payload also round-trips a byte[] entry.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testDataBindingUntypedMapWithByteArrayField() returns error? {
    check drainQueue("it.databind.untypedmapbytes.queue");
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->send({payload: {"note": "x", "data": "world".toBytes()}},
        {queueName: "it.databind.untypedmapbytes.queue"});
    check producer->close();

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.databind.untypedmapbytes.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        anydata payload = received.payload;
        test:assertTrue(payload is map<Property>, "untyped MapMessage payload should be a map<Property>");
        if payload is map<Property> {
            test:assertEquals(payload["note"], "x");
            Property? data = payload["data"];
            test:assertTrue(data is byte[], "a byte[] map entry should round-trip, not be dropped");
            if data is byte[] {
                test:assertEquals(check string:fromBytes(data), "world");
            }
        }
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

// TC-DATABIND-07: {temporary: true} binds to a real broker-created temporary queue with no error
// — a capability that didn't exist at all before this fix.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testCreateTemporaryQueueConsumer() returns error? {
    MessageConsumer|Error result = new (brokerUrl,
        username = username, password = password, destination = {queueName: "", temporary: true});
    test:assertTrue(result is MessageConsumer,
        "a temporary: true destination should bind to a real broker-created temporary queue");
    if result is MessageConsumer {
        check result->close();
    }
}

// TC-DATABIND-08: a received message's replyTo, when it's a real TemporaryQueue, preserves its
// native identity — replying to it must be delivered to the actual original queue, not a
// freshly re-created regular queue that merely shares its (broker-assigned) name.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testTemporaryReplyToRoundTripsIdentity() returns error? {
    check drainQueue("it.temp.reply.request.queue");
    MessageConsumer responder = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.temp.reply.request.queue"});

    future<string> replyFuture = start sendRequestAndAwaitTemporaryReply(
        brokerUrl, "it.temp.reply.request.queue", "request payload", 8000);

    Message? request = check responder->receive(5000);
    test:assertTrue(request is Message, "responder should receive the request");
    if request is Message {
        Destination? replyTo = request.replyTo;
        test:assertTrue(replyTo is Queue, "replyTo should be a queue destination");
        if replyTo is Queue {
            test:assertTrue(replyTo.temporary, "replyTo should be marked temporary");
            MessageProducer producer = check new (brokerUrl, username = username, password = password);
            check producer->send({payload: "reply payload"}, replyTo);
            check producer->close();
        }
    }
    check responder->close();

    string|error reply = wait replyFuture;
    test:assertEquals(reply, "reply payload",
        "reply should be delivered to the original temporary queue, not a re-created regular one");
}

// TC-DATABIND-09: a property of a type outside the 8 JMS explicitly supports degrades to a
// best-effort toString() representation instead of being silently dropped.
@test:Config {
    groups: ["integration", "dataBinding"]
}
function testUnrecognizedPropertyTypeFallsBackToString() returns error? {
    check drainQueue("it.databind.unsupportedprop.queue");
    check sendMessageWithUnsupportedPropertyType(
        brokerUrl, "it.databind.unsupportedprop.queue", "payload with odd property");

    MessageConsumer consumer = check new (brokerUrl,
        username = username, password = password,
        destination = {queueName: "it.databind.unsupportedprop.queue"});
    Message? received = check consumer->receive(5000);
    check consumer->close();
    test:assertTrue(received is Message, "should receive the sent message");
    if received is Message {
        map<Property>? props = received.properties;
        test:assertTrue(props is map<Property>, "properties should be present");
        if props is map<Property> {
            Property? unsupported = props["unsupportedProp"];
            test:assertTrue(unsupported is string,
                "an unrecognized property type should fall back to a string, not be dropped");
            if unsupported is string {
                test:assertTrue(unsupported.length() > 0, "the fallback string should be non-empty");
            }
        }
    }
}
