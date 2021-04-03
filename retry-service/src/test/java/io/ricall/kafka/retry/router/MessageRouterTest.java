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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.ricall.kafka.retry.service.RetryService.RETRY_TIME;
import static io.ricall.kafka.retry.service.RetryService.RETRY_TOPIC;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MessageRouterTest {

    private MessageRouter resolver;

    @BeforeEach
    public void init() {
        final List<DelayTopic> delayTopics = List.of("1S", "5S", "17S", "59S").stream()
                .map(v -> DelayTopic.builder()
                        .topic("test.retry" + v.toLowerCase(Locale.ROOT))
                        .delay(Duration.parse("PT" + v))
                        .build())
                .collect(Collectors.toList());

        resolver = new MessageRouter(KafkaProperties.builder()
                .deadLetterTopic("test.dlq")
                .delayTopics(delayTopics)
                .build());
    }

    @Test
    public void verifyDeliveryTimeWithMissingTimestampThrowsAnException() {
        MissingHeaderException exception = assertThrows(
                MissingHeaderException.class,
                () -> getDeliveryTimeForMessage(emptyMap()));

        assertThat(exception).hasMessage("Required header is missing: kafka_receivedTimestamp");
    }

    @Test
    public void verifyDeliveryTimeWithDefaultDelay() {
        long deliveryTime = getDeliveryTimeForMessage(Map.of(KafkaHeaders.RECEIVED_TIMESTAMP, 60_000L));

        assertThat(deliveryTime).isEqualTo(77_000L);
    }

    @Test
    public void verifyDeliveryTimeWithSpecificDelay() {
        long deliveryTime = getDeliveryTimeForMessage(Map.of(
                KafkaHeaders.RECEIVED_TIMESTAMP, 60_000L,
                RetryService.RETRY_DELAY, "PT1.234S"));

        assertThat(deliveryTime).isEqualTo(61_234L);
    }

    @Test
    public void verifyRoutingRequiresADeliveryTime() {
        MissingHeaderException exception = assertThrows(MissingHeaderException.class, () -> getDeliveryTimeForMessage(emptyMap()));

        assertThat(exception).hasMessage("Required header is missing: kafka_receivedTimestamp");
    }

    @Test
    public void verifyDeliveryWithInvalidDelayThrowsAnException() {
        DateTimeParseException exception = assertThrows(DateTimeParseException.class, () -> getDeliveryTimeForMessage(Map.of(
                KafkaHeaders.RECEIVED_TIMESTAMP, 60_000L,
                RetryService.RETRY_DELAY, "-- INVALID --")));

        assertThat(exception).hasMessage("Text cannot be parsed to a Duration");
    }

    @Test
    public void verifyMessageWithoutRetryTimeThrowsAnException() {
        MissingHeaderException exception = assertThrows(MissingHeaderException.class, () -> getTopicToRouteMessageTo(emptyMap()));

        assertThat(exception).hasMessage("Required header is missing: retry_Time");
    }

    @Test
    public void verifyMessageWithoutRetryTopicIsDeliveredToDlq() {
        String topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, 1000L));

        assertThat(topic).isEqualTo("test.dlq");
    }

    @Test
    public void verifyMessageIsDeliveredToTheRetryTopic() {
        long now = System.currentTimeMillis();

        String topic = getTopicToRouteMessageTo(Map.of(
                RETRY_TIME, now,
                RETRY_TOPIC, "test.send-to"));

        assertThat(topic).isEqualTo("test.send-to");
    }

    @Test
    public void verifyMessageDeliveryBelow1000msIsSentToRetry1s() {
        long now = System.currentTimeMillis();

        String topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 100));
        assertThat(topic).isEqualTo("test.retry1s");

        topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 850));
        assertThat(topic).isEqualTo("test.retry1s");
    }

    @Test
    public void verifyMessageDeliveryBelow5000msIsSentToRetry1s() {
        long now = System.currentTimeMillis();

        String topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 1100));
        assertThat(topic).isEqualTo("test.retry1s");

        topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 4850));
        assertThat(topic).isEqualTo("test.retry1s");
    }

    @Test
    public void verifyMessageDeliveryBelow17000IsSentToRetry5s() {
        long now = System.currentTimeMillis();

        String topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 5100));
        assertThat(topic).isEqualTo("test.retry5s");

        topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 16850));
        assertThat(topic).isEqualTo("test.retry5s");
    }

    @Test
    public void verifyMessageDeliveryBelow59000IsSentToRetry17s() {
        long now = System.currentTimeMillis();

        String topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 17100));
        assertThat(topic).isEqualTo("test.retry17s");

        topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 58850));
        assertThat(topic).isEqualTo("test.retry17s");
    }

    @Test
    public void verifyMessageDeliveryAbove59000IsSentToRetry59s() {
        long now = System.currentTimeMillis();

        String topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, now + 59100));
        assertThat(topic).isEqualTo("test.retry59s");

        topic = getTopicToRouteMessageTo(Map.of(RETRY_TIME, Long.MAX_VALUE));
        assertThat(topic).isEqualTo("test.retry59s");
    }

    private long getDeliveryTimeForMessage(Map<String, Object> headers) {
        return resolver.getDeliveryTimeForMessage(new MessageHeaders(headers));
    }

    private String getTopicToRouteMessageTo(Map<String, Object> headers) {
        return resolver.getTopicToRouteMessageTo(new MessageHeaders(headers));
    }

}
