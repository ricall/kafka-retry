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
import io.ricall.kafka.retry.router.MissingHeaderException;
import io.ricall.kafka.retry.service.RetryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static org.springframework.kafka.support.KafkaHeaders.ACKNOWLEDGMENT;

/**
 * Looses messages on restart :(
 *
 * acks need to be made in sequence
 */
@RequiredArgsConstructor
public abstract class AbstractReactiveRetryTopicListener implements Function<Flux<Message<byte[]>>, Flux<Message<byte[]>>> {

    private final Logger log;
    private final DelayTopic configuration;
    private final MessageRouter topicResolver;

    @Override
    public Flux<Message<byte[]>> apply(Flux<Message<byte[]>> messages) {
        return messages
                .flatMap(this::handleMessage)
                .onErrorContinue(this::onError);
    }

    private Mono<Message<byte[]>> handleMessage(Message<byte[]> message) {
        final MessageHeaders headers = message.getHeaders();

        final Acknowledgment acknowledgment = Optional.ofNullable(headers.get(ACKNOWLEDGMENT, Acknowledgment.class))
                .orElseThrow(() -> new MissingHeaderException(ACKNOWLEDGMENT));
        String newTopic = topicResolver.getTopicToRouteMessageTo(headers);
        if (configuration.getTopic().equals(newTopic)) {
            long delay = Optional.ofNullable(headers.get(RetryService.RETRY_TIME, Long.class))
                    .orElseThrow(MissingHeaderException::new) - System.currentTimeMillis() - 50;

            if (delay > 0) {
                delay = Math.min(delay, configuration.getDelay().toMillis());
                return Mono.just(message)
                        .delayElement(Duration.ofMillis(delay))
                        .map(this::sendToNextTopic)
                        .doOnNext(m -> acknowledgment.acknowledge());
            }
            newTopic = topicResolver.getTopicToRouteMessageTo(headers);

        }
        return Mono.just(MessageBuilder.fromMessage(message)
                .setHeader("spring.cloud.stream.sendto.destination", newTopic)
                .build())
            .doOnNext(m -> acknowledgment.acknowledge());
    }

    private Message<byte[]> sendToNextTopic(Message<byte[]> message) {
        final MessageHeaders headers = message.getHeaders();

        return MessageBuilder.fromMessage(message)
                .setHeader("spring.cloud.stream.sendto.destination", topicResolver.getTopicToRouteMessageTo(headers))
                .build();
    }

    private void onError(Throwable t, Object object) {
        log.info("Listener Error", t);
    }
}
