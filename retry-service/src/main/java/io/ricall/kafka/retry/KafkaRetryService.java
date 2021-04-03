package io.ricall.kafka.retry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Slf4j
@EnableKafka
@SpringBootApplication
@RequiredArgsConstructor
@ConfigurationPropertiesScan
public class KafkaRetryService {

    public static void main(String[] args) {
        SpringApplication.run(KafkaRetryService.class, args);
    }

    @Bean
    public Consumer<Message<byte[]>> dlqListener() {
        return message -> log.info(" !!!!! DLQ Received {}", message);
    }

}
