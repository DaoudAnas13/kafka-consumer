package com.example.productnotification.config;

import com.example.productnotification.dto.ProductCreatedEvent;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    // Auto-configured by Spring Boot from application.yml — injected here
    // so we can customize the listener container factory.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductCreatedEvent>
    kafkaListenerContainerFactory(ConsumerFactory<String, ProductCreatedEvent> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, ProductCreatedEvent>();
        factory.setConsumerFactory(consumerFactory);

        // Offset is committed ONLY when the listener calls ack.acknowledge()
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // On failure: retry 3 times, 1s apart, then give up WITHOUT committing the offset.
        var errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        errorHandler.setAckAfterHandle(false);   // don't commit a record that failed
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}