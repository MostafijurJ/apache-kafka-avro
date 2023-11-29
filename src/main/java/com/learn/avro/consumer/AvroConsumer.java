package com.learn.avro.consumer;

import com.learn.avro.schema.EventMessage;
import com.learn.avro.schema.RuleMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@Slf4j
public class AvroConsumer {


    @KafkaListener(topics = "${avro.event.topic.name}", containerFactory = "eventMessageKafkaListenerContainerFactory")
    public void consumeEventMessage(EventMessage eventMessage) {
       log.info("Event Message Consumed -> " + eventMessage.getStatus()+ " at -> "+ new Date());
    }


    @KafkaListener(topics = "${avro.rule.topic.name}", containerFactory = "ruleMessageKafkaListenerContainerFactory")
    public void consumeRuleMessage(RuleMessage ruleMessage) {
        log.info("Rule Message Consumed -> " + ruleMessage.getModel());
    }


    @KafkaListener(topics = "${avro.success.topic.name}", containerFactory = "eventMessageKafkaListenerContainerFactory")
    public void consumeSuccessEventMessage(EventMessage eventMessage) {
        log.warn("Success Event Message Consumed -> " + eventMessage.getStatus()+ " at -> "+ new Date());
    }


    @KafkaListener(topics = "${avro.failed.topic.name}", containerFactory = "eventMessageKafkaListenerContainerFactory")
    public void consumeFailedEventMessage(EventMessage eventMessage) {
        log.warn("Failed Event Message Consumed -> " + eventMessage.getStatus()+ " at -> "+ new Date());
    }

    @KafkaListener(topics = "${avro.other.topic.name}", containerFactory = "eventMessageKafkaListenerContainerFactory")
    public void consumeOthersEventMessage(EventMessage eventMessage) {
        log.warn("Others Event Message Consumed -> " + eventMessage.getStatus()+ " at -> "+ new Date());
    }

}
