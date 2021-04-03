package io.ricall.kafka.client;

import io.ricall.kafka.client.service.MessageSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@EnableKafka
@SpringBootApplication
@RequiredArgsConstructor
@ConfigurationPropertiesScan
public class KafkaClientApplication {

    private final MessageSender sender;

    public static void main(String[] args) {
        SpringApplication.run(KafkaClientApplication.class, args);
    }

    private AtomicInteger count = new AtomicInteger();

    @Bean
    public Consumer<Message<byte[]>> deliveryTopic1() {
        return displayMessageInfo(1);
    }

    @Bean
    public Consumer<Message<byte[]>> deliveryTopic2() {
        return displayMessageInfo(2);
    }

    private Consumer<Message<byte[]>> displayMessageInfo(int index) {
        return message -> {
            final MessageHeaders headers = message.getHeaders();
            long offset = System.currentTimeMillis() - Optional.ofNullable(headers.get(RetryHeaders.RETRY_TIME, Long.class)).orElse(0L);
            log.info("deliveryTopic{}({}): {} Delivered after delay {} -> {}",
                    index,
                    count.incrementAndGet(),
                    new String(message.getPayload()),
                    Optional.ofNullable(headers.get(RetryHeaders.RETRY_DELAY, String.class)).orElse(""),
                    offset);
//            log.info("         {}", message);
        };
    }

    @Bean
    CommandLineRunner onStartup() {
        int items = 1_000;

        return args -> {
            log.info("Started Application - Sending {} records", items);
            Flux.range(1, items)
                    .delayElements(Duration.ofMillis(40))
                    .doOnTerminate(() -> log.info("Finished - All records sent"))
                    .subscribe(this::sendMessage);
        };
    }

    private void sendMessage(int index) {
        String payload = "Test Message " + index;
        String header = "" + index;
        final Message<byte[]> message = MessageBuilder.withPayload(payload.getBytes(StandardCharsets.UTF_8))
                .setHeader(KafkaHeaders.MESSAGE_KEY, header.getBytes(StandardCharsets.UTF_8))
                .setHeader(RetryHeaders.RETRY_DELAY, "PT" + new Random().nextInt(60) + "S")
                .setHeader(RetryHeaders.RETRY_TOPIC, "delivery.topic" + (index % 2))
                .build();

        sender.sendMessage("retry.topic", message);
    }

}
