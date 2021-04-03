# kafka-retry
Sample project for trialing a generic retry microservice that can be used
to support delayed delivery of messages

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

### Outstanding work
* Add support for linear/exponential retries
* Tidyup code
* Add consumption pausing/resuming (this will be needed for topics that have large delays eg. 10 minutes/1 hour)
* Add support for creating the Retry Topic Listeners using a bean factory. (We need to ensure that the Spring Cloud
  Stream post processor runs on the beans we create)
* Create a scs reactive version - using Function<Flux<Message<byte[]>>, Flux<Message<byte[]>>>
* Add entity persistence to kafka-client