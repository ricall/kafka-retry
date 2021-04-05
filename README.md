# kafka-retry
Sample project for trialing a generic retry microservice that can be used
to support delayed delivery of messages in Kafka

### Retry Logic
All messages are sent to a `retry.topic` kafka topic. Each message should have the following headers set:
- RetryDelay: String (In Duration format eg. PT1H, PT2S, PT1.400S)
- RetryType: Enum String (Linear|Exponential)
- RetryMax: String (if not provided 5)
- RetryTopic: String (topic to resend to)
- DLQTopic: String (topic to send failed retries to)

The retry logic will then:
- Generate new headers
  * RetryCount: 0 (to start with)
  * RetryTime: <message time> + RetryType(Retry Count, Retry Delay) - Linear or Exponential Backoff
- Put the message on the topic with the largest delay that is less than RetryTime - now

eg.
`retry.internal.delay1` - has 1 second delay
`retry.internal.delay2` - has 5 second delay
`retry.internal.delay3` - has 17 second delay
`retry.internal.delay4` - has 59 second delay

So if we want to delay for 1:34
- once on `retry.internal.delay4` = 35 seconds remaining
- twice on `retry.internal.delay3` = 1 second remaining
- once on `retry.interval.delay1` = Message is delivered to RetryTopic

The processing on each `retry.internal.delay*` queue is the same

- Poll for message - RetryDelay = RetryTime - now
- if message RetryDelay < 0  - then deliver message to Retry Topic - and keep polling
- Pick Topic to deliver the message to:
  * If it is another topic - send the message to that topic
  * Else if it is the topic we are listening on
    * If MaxRetryDelay < RetryDelay:
      * Wait MaxRetryDelay, and then recalculate RetryDelay and pick the topic to deliver to and send it
    * else
      * Wait RetryDelay and then delivery message to Retry Topic
    
Whenever the consumer needs to wait it will perform the following actions:
* Pause consuming on this topic/partition
* Wait the required time
* Resume consuming on this topic/partition

### Implementation
The code is implemented using Spring Cloud Streams (SCS) using a Kafka binder to pass messages to Kafka.

This implementation allows the code to use simple Java interfaces for our Topic processors:
* `Function<Message<byte[]>, Message<byte[]>>` - A function that takes a message and returns another message, and
* `Function<Flux<Message<byte[]>>, Flux<Message<byte[]>>` - A function that takes a flux of messsage and returns
  a flux of message. This implementation uses reactive streams to 

### Testing the implementations
There are two implementations of the above logic:

#### Synchronous Listener
All waiting is done using `Thread.sleep()`. A window is maintained to determine if messages should be delayed or not.
As soon as we get a message that was queued after our window ends we advance the window.

The current implementation requires that the poll completes in less than 5 minutes. This is unlikely to be a problem
unless we cannot process 500 messages in < 5 minutes.

to use the synchronous listener:
```yaml
consumer:
  reactive: false

spring:
  cloud:
    stream:
      default:
        consumer:
          concurrency: 4
```

#### Asynchronous Listener
All waiting is done using `delayElement()` on the reactive stream. The algorithm is still the same with messages being
processed in sequence and moved onto different retry topics as needed.

Because we are using reactive streams we can only have a single consumer thread per topic but the delivered messages
are typically delivered much closer to the expected time.

to use the asynchronous listener:

```yaml
consumer:
  reactive: true

spring:
  cloud:
    stream:
      default:
        consumer:
          concurrency: 1
```

#### Starting the underlying infrastructure
```bash
make start-dev
```

This will start the following services:
- database
- zookeeper
- kafka broker
- schema-registry
- confluent control centre

These need to be started and in a `healthy` state before you can start tests: http://localhost:9021/clusters

#### Starting the retry service
```bash
./gradlew retry-service:bootRun
```

#### Starting the retry client
```bash
./gradlew retry-client:bootRun
```

This will send `1,000` messages to the `retry.topic` with different redelivery delays. The messages will be sent back to
either `delivery.topic0` or `delivery.topic1` which the retry client listens to and displays the following:

```bash
2021-04-05 16:45:22.127  INFO 22778 --- [container-0-C-1] i.r.kafka.client.KafkaClientApplication  : 1(1000): Test Message 999 Delivered after delay PT24S -> -19 [retry.internal.delay4 > PT17S|23988|retry.internal.delay3|7008 > PT7S|7003|delivery.topic1|23]
```

This shows:
- The delivery topic the message was received on: 1 (delivery.topic.1) 
- The message count: 1000
- The message payload: Test Message 999
- The retry delay: P24S (delay 24 seconds)
- The offset: -19 (delivered 19ms early due to the Jitter configuration)
- The topics the message has been queued on: [retry.internal.delay4 > PT17S|23988|retry.internal.delay3|7008 > PT7S|7003|delivery.topic1|23]
   The message was processed on `retry.internal.delay4` (17 second delay topic)
      - arrived with 23,988ms delay needed
      - delayed 17 seconds
      - sent to retry.internal.delay3 with 7,008ms delay needed
   The message was processed on `retry.internal.delay3` (7 second delay topic)
      - arrived with 7,003ms delay needed
      - delayed 7 seconds
      - send to delivery.topic1 with 23ms delay needed

#### Stress testing the retry service
Start the retry-service and retry-client applications.

When messages are flowing stop the retry-service (ctrl+c) then restart it again. Keep doing this until no more messages are flowing

Confirm that the retry-client has received all the messages (there should be 1000).

### Outstanding work
* Add support for linear/exponential retries
* Tidyup code
* Add consumption pausing/resuming (this will be needed for topics that have poll cycles that take > 5 minutes)
* Add support for creating the Retry Topic Listeners using a bean factory. (We still need to ensure that the Spring Cloud
  Stream post processor runs on the beans we create)
* Add entity persistence to kafka-client - we can then query the database using SQL to determine how well each algorithm works.
