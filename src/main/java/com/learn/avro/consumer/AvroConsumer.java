package com.learn.avro.consumer;

import com.learn.avro.schema.EventMessage;
import com.learn.avro.schema.RuleMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AvroConsumer {


    @KafkaListener(topics = "${avro.event.topic.name}", containerFactory = "kafkaListenerContainerFactory")
    public void consumeEventMessage(EventMessage eventMessage) {
        System.out.println("Consumed event -> " + eventMessage);
       log.info("Consumed event details -> " + eventMessage.getMachine());
    }


    @KafkaListener(topics = "${avro.rule.topic.name}", containerFactory = "ruleKafkaListenerContainerFactory")
    public void consumeRuleMessage(RuleMessage ruleMessage) {
        System.out.println("rule event consumed -> " + ruleMessage);
        log.info("Rule event consumed -> " + ruleMessage.getModel());
    }


}
