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

import ballerina/lang.runtime;
import ballerina/test;

isolated int itCleanupListenerCount = 0;

// TC-CLEANUP-01: Producer close is idempotent — double-close must not panic
@test:Config {
    groups: ["integration", "cleanup"]
}
function testItProducerCloseIdempotent() returns error? {
    MessageProducer producer = check new (brokerUrl, username = username, password = password);
    check producer->close();
    // Second close: () or Error are both acceptable; what matters is no panic.
    do { check producer->close(); } on fail { }
}

// TC-CLEANUP-02: Listener close is idempotent — double-stop must not panic
@test:Config {
    groups: ["integration", "cleanup"]
}
function testItListenerCloseIdempotent() returns error? {
    Listener lst = check new (brokerUrl, username = username, password = password);
    check lst.gracefulStop();
    // Second stop: () or Error are both acceptable; what matters is no panic.
    do { check lst.gracefulStop(); } on fail { }
}

// TC-CLEANUP-03: No message loss — send 10, receive all 10, none duplicated
@test:Config {
    groups: ["integration", "cleanup"]
}
function testItNoMessageLossHappyPath() returns error? {
    check drainQueue("it.cleanup.nomsg.queue");
    lock {
        itCleanupListenerCount = 0;
    }

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    int messageCount = 10;
    foreach int i in 1 ... messageCount {
        check prod->send({
            messageId: string `it-cleanup-${i}`,
            payload: string `message-${i}`.toBytes()
        }, {queueName: "it.cleanup.nomsg.queue"});
    }
    check prod->close();

    // Receive all 10 messages synchronously to verify no loss and no duplication.
    MessageConsumer cons = check new (brokerUrl,
        username = username, password = password, destination = {queueName: "it.cleanup.nomsg.queue"});
    int received = 0;
    Message? msg = check cons->receive(5000);
    while msg is Message {
        received += 1;
        msg = check cons->receive(2000);
    }
    check cons->close();

    test:assertEquals(received, messageCount,
        string `should receive all ${messageCount} messages — none lost, none duplicated`);
}

// TC-CLEANUP-04 (bonus): Listener service cleans up resources after attach
@test:Config {
    groups: ["integration", "cleanup"]
}
function testItListenerServiceCleanup() returns error? {
    check drainQueue("it.cleanup.listener.queue");
    lock {
        itCleanupListenerCount = 0;
    }
    Listener cleanupListener = check new (brokerUrl, username = username, password = password);
    Service cleanupSvc = @ServiceConfig {
        queueName: "it.cleanup.listener.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                itCleanupListenerCount += 1;
            }
        }
    };
    check cleanupListener.attach(cleanupSvc, "it-cleanup-svc");
    check cleanupListener.'start();

    MessageProducer prod = check new (brokerUrl, username = username, password = password);
    check prod->send({
        messageId: "it-cleanup-svc-01",
        payload: "cleanup test".toBytes()
    }, {queueName: "it.cleanup.listener.queue"});
    check prod->close();

    runtime:sleep(4);

    lock {
        test:assertEquals(itCleanupListenerCount, 1, "service should receive the message");
    }
    check cleanupListener.gracefulStop();
}

// TC-CLEANUP-05: detach() removes the service from the listener's bookkeeping, so a later
// start() does not fail trying to re-register a consumer that detach() already closed.
@test:Config {
    groups: ["integration", "cleanup"]
}
function testItListenerStartAfterDetach() returns error? {
    check drainQueue("it.cleanup.detach.queue");
    Listener detachListener = check new (brokerUrl, username = username, password = password);
    Service detachSvc = @ServiceConfig {
        queueName: "it.cleanup.detach.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    check detachListener.attach(detachSvc, "it-detach-svc");
    check detachListener.'start();
    check detachListener.detach(detachSvc);

    // Previously threw a JMSException ("consumer closed") because the detached service's
    // stale entry was never removed from the listener's internal service list.
    check detachListener.'start();
    check detachListener.gracefulStop();
}

// TC-CLEANUP-06: gracefulStop() after detaching one of several attached services must not fail
// (regression guard for double-stopping a receiver that's no longer in the bookkeeping list).
@test:Config {
    groups: ["integration", "cleanup"]
}
function testItListenerGracefulStopAfterPartialDetach() returns error? {
    check drainQueue("it.cleanup.detach2.queue");
    check drainQueue("it.cleanup.detach3.queue");
    Listener partialDetachListener = check new (brokerUrl, username = username, password = password);
    Service svcA = @ServiceConfig {
        queueName: "it.cleanup.detach2.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    Service svcB = @ServiceConfig {
        queueName: "it.cleanup.detach3.queue"
    } service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    check partialDetachListener.attach(svcA, "it-detach-svc-a");
    check partialDetachListener.attach(svcB, "it-detach-svc-b");
    check partialDetachListener.'start();
    check partialDetachListener.detach(svcA);
    check partialDetachListener.gracefulStop();
}
