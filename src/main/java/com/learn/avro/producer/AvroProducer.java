package com.learn.avro.producer;

import com.learn.avro.schema.EventMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class AvroProducer {
    @Autowired
    private KafkaTemplate<String, EventMessage> kafkaTemplate;

    @Value("${avro.topic.name}")
    String topicName;



    public void send(EventMessage eventMessage) {
        CompletableFuture<SendResult<String, EventMessage>> send = kafkaTemplate.send(topicName, eventMessage);
        log.info(String.format("Produced event -> %s", eventMessage));
    }

    private void onSuccess(SendResult<String, EventMessage> result) {
        log.info(String.format("Produced event -> %s", result.getProducerRecord().value()));
    }
    private void onFailure(Throwable ex) {
        log.error(String.format("Unable to produce event : %s", ex.getMessage()));
    }
}
