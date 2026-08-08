import ballerina/io;
import ballerinax/activemq;

configurable string brokerUrl = "tcp://localhost:61616";
configurable string username = "admin";
configurable string password = "admin";

const string ORDERS_QUEUE = "examples.orders.queue";

public function main() returns error? {
    activemq:MessageConsumer consumer = check new (brokerUrl,
        username = username,
        password = password,
        destination = {queueName: ORDERS_QUEUE}
    );

    int received = 0;
    while received < 3 {
        record {|*activemq:Message; string payload;|}? msg = check consumer->receive(5000);
        if msg is () {
            io:println("No more messages within the timeout, stopping.");
            break;
        }
        io:println("Received order: ", msg.payload);
        received += 1;
    }

    check consumer->close();
}
