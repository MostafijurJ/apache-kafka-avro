package com.learn.avro.config;

import com.learn.avro.schema.EventMessage;
import com.learn.avro.schema.RuleMessage;
import com.learn.avro.serializer.AvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value(value = "${spring.kafka.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value(value = "${spring.kafka.listener.concurrency}")
    private Integer concurrency;


    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> valueType) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, EventMessage> eventMessageConsumerFactory() {
        return createConsumerFactory(EventMessage.class);
    }

    @Bean(name = "eventMessageKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, EventMessage> eventMessageKafkaListenerContainerFactory() {
        return createListenerContainerFactory(eventMessageConsumerFactory());
    }

   /* @Bean
    public ConsumerFactory<String, RuleMessage> ruleMessageConsumerFactory() {
        return createConsumerFactory(RuleMessage.class);
    }

    @Bean(name = "ruleMessageKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, RuleMessage> ruleMessageKafkaListenerContainerFactory() {
        return createListenerContainerFactory(ruleMessageConsumerFactory());
    }*/
}
