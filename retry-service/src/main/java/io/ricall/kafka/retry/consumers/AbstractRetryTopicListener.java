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

package io.ricall.kafka.retry.consumers;

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
public abstract class AbstractRetryTopicListener implements Function<Message<byte[]>, Message<byte[]>> {

    @Autowired private KafkaProperties properties;
    @Autowired private MessageRouter topicResolver;
    @Autowired private DelayService delayService;

    private volatile long windowEnd;

    @PostConstruct
    public void init() {
        windowEnd = System.currentTimeMillis() + getTopicConfiguration().getDelay().toMillis();
    }

    public abstract DelayTopic getTopicConfiguration();

    @Override
    public Message<byte[]> apply(Message<byte[]> message) {
        return handleMessage(message);
    }

    private synchronized long advanceWindowEndIfNecessary(long now, long messageTime) {
        long topicDelay = getTopicConfiguration().getDelay().toMillis();

        while (messageTime > windowEnd && windowEnd < (now + topicDelay)) {
            windowEnd += topicDelay;
        }
        return windowEnd;
    }

    private Message<byte[]> handleMessage(Message<byte[]> message) {
        final MessageHeaders headers = message.getHeaders();
        final long time = Optional.ofNullable(headers.get(RetryService.RETRY_TIME, Long.class))
                .orElseThrow(IllegalStateException::new);
        final long now = System.currentTimeMillis();
        final DelayTopic configuration = getTopicConfiguration();

        String trace = Optional.ofNullable(headers.get("_trace", String.class))
                .orElseThrow(IllegalStateException::new)
                + " > " + getTopicConfiguration().getDelay() + "|" + (time - System.currentTimeMillis()) + "|";
        String topic = topicResolver.getTopicToRouteMessageTo(headers);
        if (configuration.getTopic().equals(topic)) {
            long messageTime = Optional.ofNullable(headers.get(RECEIVED_TIMESTAMP, Long.class))
                    .orElseThrow(IllegalStateException::new);

            long currentWindowEnd = advanceWindowEndIfNecessary(now, messageTime);
            if (messageTime < currentWindowEnd) {
                long delay = time - now - properties.getJitter().toMillis();

                delay = Math.min(delay, currentWindowEnd - now);
                if (delay > 0) {
                    delayService.delayMillis(delay);
                    topic = topicResolver.getTopicToRouteMessageTo(headers);
                }
            }
        }
        trace += topic + "|" + (time - System.currentTimeMillis());
        log.info("OUT [{}] {} (delay {}ms)", trace, new String(message.getPayload()), System.currentTimeMillis() - now);
        return MessageBuilder.fromMessage(message)
                .setHeader("spring.cloud.stream.sendto.destination", topic)
                .setHeader("_trace", trace)
                .build();
    }

}
