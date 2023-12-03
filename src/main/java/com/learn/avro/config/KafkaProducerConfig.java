package com.learn.avro.config;

import com.learn.avro.schema.EventMessage;
import com.learn.avro.schema.RuleMessage;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.schema.registry.url}")
    private String schemaRegistryUrl;

    private <T> ProducerFactory<String, T> createProducerFactory(Class<T> messageType) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put("schema.registry.url", schemaRegistryUrl);
        configProps.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());
        configProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return new DefaultKafkaProducerFactory<>(configProps);
    }


    private <T> KafkaTemplate<String, T> createKafkaTemplate(Class<T> messageType) {
        return new KafkaTemplate<>(createProducerFactory(messageType));
    }

    @Bean
    public ProducerFactory<String, RuleMessage> ruleMessageProducerFactory() {
        return createProducerFactory(RuleMessage.class);
    }

    @Bean(name = "ruleMessageKafkaTemplate")
    public KafkaTemplate<String, RuleMessage> ruleMessageKafkaTemplate() {
        return createKafkaTemplate(RuleMessage.class);
    }

    @Bean
    public ProducerFactory<String, EventMessage> eventMessageProducerFactory() {
        return createProducerFactory(EventMessage.class);
    }

    @Bean(name = "eventMessageKafkaTemplate")
    public KafkaTemplate<String, EventMessage> eventMessageKafkaTemplate() {
        return createKafkaTemplate(EventMessage.class);
    }
}
