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

package io.ricall.kafka.retry.consumers.reactive;

import io.ricall.kafka.retry.configuration.KafkaProperties;
import io.ricall.kafka.retry.configuration.KafkaProperties.DelayTopic;
import io.ricall.kafka.retry.router.MessageRouter;
import io.ricall.kafka.retry.router.MissingHeaderException;
import io.ricall.kafka.retry.service.RetryService;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

/**
 * Looses messages on restart :(
 *
 * acks need to be made in sequence
 */
@Data
@Slf4j
@NoArgsConstructor
public abstract class AbstractReactiveRetryTopicListener implements Function<Flux<Message<byte[]>>, Flux<Message<byte[]>>> {

    @Autowired private KafkaProperties properties;
    @Autowired private MessageRouter topicResolver;

    public abstract DelayTopic getTopicConfiguration();

    @Override
    public Flux<Message<byte[]>> apply(Flux<Message<byte[]>> messages) {
        return messages
                .flatMap(this::handleMessage)
                .onErrorContinue(this::onError);
    }

    private Mono<Message<byte[]>> handleMessage(Message<byte[]> message) {
        final MessageHeaders headers = message.getHeaders();

        long messageDeliveryTime = Optional.ofNullable(headers.get(RetryService.RETRY_TIME, Long.class))
                .orElseThrow(MissingHeaderException::new) ;
        final long now = System.currentTimeMillis();
        String trace = Optional.ofNullable(headers.get("_trace", String.class))
                .orElseThrow(IllegalStateException::new)
                + " > " + getTopicConfiguration().getDelay() + "|" + (messageDeliveryTime - System.currentTimeMillis()) + "|";
//        final Acknowledgment acknowledgment = Optional.ofNullable(headers.get(ACKNOWLEDGMENT, Acknowledgment.class))
//                .orElseThrow(() -> new MissingHeaderException(ACKNOWLEDGMENT));

        String topic = topicResolver.getTopicToRouteMessageTo(headers);
        if (getTopicConfiguration().getTopic().equals(topic)) {

            long delay = Math.min(messageDeliveryTime - now, getTopicConfiguration().getDelay().toMillis()) - 20;
            if (delay > 0) {
                return Mono.just(message)
                        .delayElement(Duration.ofMillis(delay))
                        .map(sendToNextTopic(trace, messageDeliveryTime));
//                        .doOnNext(m -> acknowledgment.acknowledge());
            }
            topic = topicResolver.getTopicToRouteMessageTo(headers);

        }

        trace += topic + "|" + (messageDeliveryTime - System.currentTimeMillis());
        return Mono.just(MessageBuilder.fromMessage(message)
                .setHeader("spring.cloud.stream.sendto.destination", topic)
                .setHeader("_trace", trace)
                .build());
//            .doOnNext(m -> acknowledgment.acknowledge());
    }

    private Function<Message<byte[]>, Message<byte[]>> sendToNextTopic(String trace, long messageDeliveryTime) {
        return message -> {
            final MessageHeaders headers = message.getHeaders();
            final String topic = topicResolver.getTopicToRouteMessageTo(headers);

            final String newTrace = trace + topic + "|" + (messageDeliveryTime - System.currentTimeMillis());
            return MessageBuilder.fromMessage(message)
                    .setHeader("spring.cloud.stream.sendto.destination", topic)
                    .setHeader("_trace", newTrace)
                    .build();
        };
    }

    private void onError(Throwable t, Object object) {
        log.info("Listener Error", t);
    }
}
