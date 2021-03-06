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

package io.ricall.kafka.retry.service;

import io.ricall.kafka.retry.configuration.KafkaProperties;
import io.ricall.kafka.retry.router.MessageRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.Function;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryService implements Function<Message<byte[]>, Message<byte[]>> {

    public static final String PREFIX = "retry_";
    public static final String RETRY_DELAY = PREFIX + "Delay";
    public static final String RETRY_TYPE = PREFIX + "Type";
    public static final String RETRY_MAX = PREFIX + "Max";
    public static final String RETRY_TOPIC = PREFIX + "Topic";
    public static final String RETRY_DLQ = PREFIX + "DLQ";
    public static final String RETRY_COUNT = PREFIX + "Count";
    public static final String RETRY_TIME = PREFIX + "Time";
    public static final int DEFAULT_MAXIMUM_RETRIES = 5;

    private final KafkaProperties properties;
    private final MessageRouter topicResolver;

    @Override
    public Message<byte[]> apply(Message<byte[]> message) {
        final MessageHeaders headers = message.getHeaders();

        long retryTime = topicResolver.getDeliveryTimeForMessage(headers);
        String topic = topicResolver.getTopicToRouteMessageTo(headers, retryTime);
        int retryCount = Optional.ofNullable(headers.get(RETRY_COUNT, Integer.class)).orElse(0) + 1;
        if (retryCount > Optional.ofNullable(headers.get(RETRY_MAX, Integer.class)).orElse(DEFAULT_MAXIMUM_RETRIES)) {
            topic = Optional.ofNullable(headers.get(RetryService.RETRY_DLQ, String.class))
                    .orElse(properties.getDeadLetterTopic());
        }

        return MessageBuilder.fromMessage(message)
                .setHeader(RETRY_TIME, retryTime)
                .setHeader(RETRY_COUNT, retryCount)
                .setHeader("_trace", topic)
                .setHeader("spring.cloud.stream.sendto.destination", topic)
                .build();
    }

}
