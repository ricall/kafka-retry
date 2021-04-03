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

import io.ricall.kafka.retry.configuration.KafkaProperties.DelayTopic;
import io.ricall.kafka.retry.router.MessageRouter;
import io.ricall.kafka.retry.service.RetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Optional;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractRetryTopicListener implements Function<Message<byte[]>, Message<byte[]>> {

    private final DelayTopic configuration;
    private final MessageRouter topicResolver;

    private void delayMillis(long millis) {
//        log.info("Waiting {}ms", millis);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.warn("Retry thread interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Message<byte[]> apply(Message<byte[]> message) {
//        log.info(" +++++ ({}) Received message: {}", configuration, message);

        final MessageHeaders headers = message.getHeaders();
        String newTopic = topicResolver.getTopicToRouteMessageTo(headers);
        if (configuration.getTopic().equals(newTopic)) {
            long delay = Optional.ofNullable(headers.get(RetryService.RETRY_TIME, Long.class))
                    .orElseGet(System::currentTimeMillis) - System.currentTimeMillis() - 50;

            if (delay > 0) {
                delayMillis(Math.min(delay, configuration.getDelay().toMillis()));
            }
            newTopic = topicResolver.getTopicToRouteMessageTo(headers);
        }
        return MessageBuilder.fromMessage(message)
                .setHeader("spring.cloud.stream.sendto.destination", newTopic)
                .build();
    }
}
