package com.learn.avro.config;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.learn.avro.schema.EventMessage;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.KafkaStreamBrancher;

import java.util.Collections;
import java.util.Map;


@Configuration
@EnableKafka
@EnableKafkaStreams
public class kafkaStreamTopicSplitConfig{

    @Value("${avro.event.topic.name}")
    private String eventMessageTopicName;

    @Value("${avro.success.topic.name}")
    private String successEventMessageTopicName;

    @Value("${avro.failed.topic.name}")
    private String failEventMessageTopicName;

    @Value("${avro.other.topic.name}")
    private String otherEventMessageTopicName;

    @Value("${spring.kafka.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KStream<String, EventMessage> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, EventMessage> stream = streamsBuilder.stream(eventMessageTopicName, Consumed.with(Serdes.String(), iotSerde()));

        new KafkaStreamBrancher<String, EventMessage>()
                .branch((key, value) -> "Success".contentEquals(value.getStatus()), ks -> ks.to(successEventMessageTopicName))
                .branch((key, value) -> "Fail".contentEquals(value.getStatus()), ks -> ks.to(failEventMessageTopicName))
                .branch((key, value) -> "Others".contentEquals(value.getStatus()), ks -> ks.to(otherEventMessageTopicName))
                .defaultBranch(ks -> ks.to("acting-events"))
                .onTopOf(stream);

        return stream;
    }

    @Bean
    public Serde<EventMessage> iotSerde() {
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        SpecificAvroSerde<EventMessage> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, false);

        return Serdes.serdeFrom(serde.serializer(), serde.deserializer());
    }


}

