package com.learn.avro.consumer;

import com.learn.avro.schema.EventMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AvroConsumer {


    @KafkaListener(topics = "${avro.event.topic.name}", containerFactory = "eventMessageKafkaListenerContainerFactory")
    public void consumeEventMessage(EventMessage eventMessage) {
        System.out.println("Consumed event -> " + eventMessage);
        log.info("Event Message Consumed -> " + eventMessage.getMachine());
    }


/*    @KafkaListener(topics = "${avro.rule.topic.name}", containerFactory = "ruleMessageKafkaListenerContainerFactory")
    public void consumeRuleMessage(RuleMessage ruleMessage) {
        System.out.println("rule event consumed -> " + ruleMessage);
        log.info("Rule Message Consumed -> " + ruleMessage.getModel());
    }*/


}
