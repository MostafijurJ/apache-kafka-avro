package com.learn.avro.producer;

import com.learn.avro.schema.EventMessage;
import com.learn.avro.schema.RuleMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AvroProducer {

    @Autowired
    @Qualifier("ruleMessageKafkaTemplate")
    private KafkaTemplate<String, RuleMessage> ruleMessageKafkaTemplate;

    @Autowired
    @Qualifier("eventMessageKafkaTemplate")
    private KafkaTemplate<String, EventMessage> eventMessageKafkaTemplate;

    @Value("${avro.event.topic.name}")
    String eventMessageTopicName;

    @Value("${avro.rule.topic.name}")
    String ruleMessageTopicName;


    public void sendMessageEvent(EventMessage eventMessage) {
        eventMessageKafkaTemplate.send(eventMessageTopicName, eventMessage);
        log.info(String.format("Produced EventMessage -> %s", eventMessage));
    }

    public void sendRuleEvent(RuleMessage eventMessage) {
        ruleMessageKafkaTemplate.send(eventMessageTopicName, eventMessage);
        log.info(String.format("Produced RuleMessage -> %s", eventMessage));
    }


}
