# kafka-streams-demo

By Alexander Kazimirsky

To run demo you need Docker environment set up on your PC - it will raise Kafka container
Checkout the code to your IDE and run the project (maven required).

There are 5 types of messages. Each message contains a random Float business value from 0 to 10.
The demo sets up N message generators that generate messages in random intervals of time (1>t>2 seconds).
These messages are sent to kafka topic.

Messages are consumed and streamed to KTable windowed by 5 seconds.
Messages are grouped by type and their business values are aggregated.

Windows of grouped and aggregated values are printed as windows are closed each 5 seconds.