package com.example.productnotification.config;

import com.example.productnotification.dto.ProductCreatedEvent;
import com.example.productnotification.exception.PoisonMessageException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductCreatedEvent>
    kafkaListenerContainerFactory(ConsumerFactory<String, ProductCreatedEvent> consumerFactory,
                                  DefaultErrorHandler errorHandler) {   // 1. new parameter

        var factory = new ConcurrentKafkaListenerContainerFactory<String, ProductCreatedEvent>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        factory.setCommonErrorHandler(errorHandler);                    // 2. use the bean

        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        // Retry transient failures: 1s interval, 3 retry attempts (4 total deliveries).
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);

        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterRecoverer(), backOff);
        handler.setCommitRecovered(true);

        // Classification: these go to DLT immediately, without burning retries.
        handler.addNotRetryableExceptions(PoisonMessageException.class);

        // Visibility: log every retry attempt.
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for record [{}]: {}",
                        deliveryAttempt, record.key(), ex.getMessage()));

        return handler;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterRecoverer() {
        return new DeadLetterPublishingRecoverer(dltKafkaTemplate());
    }

    /**
     * Dedicated template for DLT publishing. ByteArray serializers pass the
     * ORIGINAL record bytes through untouched — safe even if the value isn't
     * valid JSON for our ProductCreatedEvent (e.g. a poison payload).
     */
    @Bean
    public KafkaTemplate<Object, Object> dltKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");

        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(String.class, new StringSerializer());
        delegates.put(ProductCreatedEvent.class, new JsonSerializer<ProductCreatedEvent>());

        var serializer = new DelegatingByTypeSerializer(delegates);
        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(props, serializer, serializer));
    }

    /** The DLT itself — same partition count as the source topic. */
    @Bean
    public NewTopic productEventsDlt() {
        return TopicBuilder.name("product-events.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }
}