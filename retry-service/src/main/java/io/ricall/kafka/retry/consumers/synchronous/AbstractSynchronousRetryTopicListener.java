/*
 * Copyright (c) 2021 Richard Allwood
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.ricall.kafka.retry.consumers.synchronous;

import io.ricall.kafka.retry.configuration.KafkaProperties;
import io.ricall.kafka.retry.configuration.KafkaProperties.DelayTopic;
import io.ricall.kafka.retry.router.MessageRouter;
import io.ricall.kafka.retry.service.DelayService;
import io.ricall.kafka.retry.service.RetryService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.PostConstruct;
import java.util.Optional;
import java.util.function.Function;

import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP;

@Data
@Slf4j
public abstract class AbstractSynchronousRetryTopicListener implements Function<Message<byte[]>, Message<byte[]>> {

    @Autowired private KafkaProperties properties;
    @Autowired private MessageRouter topicResolver;
    @Autowired private DelayService delayService;

    private volatile long windowStart;
    private volatile long windowEnd;

    @PostConstruct
    public void init() {
        windowStart = 0;
        windowEnd = System.currentTimeMillis() + getTopicConfiguration().getDelay().toMillis();
    }

    public abstract DelayTopic getTopicConfiguration();

    private boolean isSameDestinationTopic(String topic) {
        return getTopicConfiguration().getTopic().equals(topic);
    }

    private synchronized long advanceWindowEndIfNecessary(long now, long messageTime) {
        long topicDelay = getTopicConfiguration().getDelay().toMillis();

        while (messageTime > windowEnd && windowEnd < (now + topicDelay)) {
            windowStart = windowEnd;
            windowEnd += topicDelay;
        }
        return windowEnd;
    }

    private synchronized boolean inWindow(long messageTime) {
        return messageTime > windowStart;
    }

    @Override
    public Message<byte[]> apply(Message<byte[]> message) {
        final MessageHeaders headers = message.getHeaders();

        final long messageDeliveryTime = Optional.ofNullable(headers.get(RetryService.RETRY_TIME, Long.class))
                .orElseThrow(IllegalStateException::new);
        final long now = System.currentTimeMillis();

        String trace = Optional.ofNullable(headers.get("_trace", String.class))
                .orElseThrow(IllegalStateException::new)
                + " > " + getTopicConfiguration().getDelay() + "|" + (messageDeliveryTime - System.currentTimeMillis()) + "|";

        String topic = topicResolver.getTopicToRouteMessageTo(headers);
        if (isSameDestinationTopic(topic)) {
            // Message will be sent back to this topic so we need to ensure that we are delaying
            // the message
            long messageTime = Optional.ofNullable(headers.get(RECEIVED_TIMESTAMP, Long.class))
                    .orElseThrow(IllegalStateException::new);

            long currentWindowEnd = advanceWindowEndIfNecessary(now, messageTime);
            if (inWindow(messageTime)) {
                // Ensure that any delays will finish on the *current-window-end* (minus 20ms)
                long delay = Math.min(messageDeliveryTime - now , currentWindowEnd - now) - 20;
                if (delay > 0) {
                    // We can only delay while we are within the current window
                    delayService.delayMillis(delay);
                    // after the delay we may need to send the message to another topic
                    topic = topicResolver.getTopicToRouteMessageTo(headers);
                }
            }
        }

        trace += topic + "|" + (messageDeliveryTime - System.currentTimeMillis());
        log.info("OUT [{}] {} (delay {}ms)", trace, new String(message.getPayload()), System.currentTimeMillis() - now);

        return MessageBuilder.fromMessage(message)
                .setHeader("spring.cloud.stream.sendto.destination", topic)
                .setHeader("_trace", trace)
                .build();
    }

}
