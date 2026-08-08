import ballerina/log;
import ballerinax/activemq;

configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";

const string ORDERS_QUEUE = "examples.orders.queue";

listener activemq:Listener mqListener = check new (brokerUrl, username = username, password = password);

@activemq:ServiceConfig {
    queueName: ORDERS_QUEUE
}
service activemq:Service on mqListener {
    remote function onMessage(record {|*activemq:Message; string payload;|} message) returns error? {
        log:printInfo("Processing order", payload = message.payload);
    }
}
