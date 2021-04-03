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

package io.ricall.kafka.retry.router;

import io.ricall.kafka.retry.configuration.KafkaProperties;
import io.ricall.kafka.retry.configuration.KafkaProperties.DelayTopic;
import io.ricall.kafka.retry.service.RetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

import static io.ricall.kafka.retry.service.RetryService.RETRY_DELAY;
import static io.ricall.kafka.retry.service.RetryService.RETRY_TIME;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageRouter {
    public static final String DEFAULT_DELIVERY_DELAY = "PT17S";

    private final KafkaProperties properties;

    public long getDeliveryTimeForMessage(MessageHeaders headers) {
        long timestamp = Optional.ofNullable(headers.get(RECEIVED_TIMESTAMP, Long.class))
                .orElseThrow(() -> new MissingHeaderException(RECEIVED_TIMESTAMP));
        final String duration = Optional.ofNullable(headers.get(RETRY_DELAY, String.class))
                .orElse(DEFAULT_DELIVERY_DELAY);

        return timestamp + Duration.parse(duration).toMillis();
    }

    public String getTopicToRouteMessageTo(MessageHeaders headers) {
        long deliveryTime = Optional.ofNullable(headers.get(RETRY_TIME, Long.class))
                .orElseThrow(() -> new MissingHeaderException(RETRY_TIME));

        return getTopicToRouteMessageTo(headers, deliveryTime);
    }

    public String getTopicToRouteMessageTo(MessageHeaders headers, long deliveryTime) {
        String dlq = Optional.ofNullable(headers.get(RetryService.RETRY_DLQ, String.class))
                .orElse(properties.getDeadLetterTopic());

        deliveryTime -= properties.getJitter().toMillis();

        long now = System.currentTimeMillis();
        if (deliveryTime <= now) {
            return Optional.ofNullable(headers.get(RetryService.RETRY_TOPIC, String.class)).orElse(dlq);
        }

        String topic = properties.getDelayTopics().get(0).getTopic();
        for (DelayTopic delayTopic : properties.getDelayTopics()) {
            long candidateExpiry = now + delayTopic.getDelay().toMillis();
            if (candidateExpiry > deliveryTime) {
                return topic;
            }
            topic = delayTopic.getTopic();
        }
        return topic;
    }

}