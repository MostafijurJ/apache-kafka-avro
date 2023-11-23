package com.learn.avro.controller;

import com.learn.avro.producer.AvroProducer;
import com.learn.avro.schema.EventMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/avro")
public class AvroController {

    @Autowired
    private AvroProducer avroProducer;
    @GetMapping("/send")
    void sentMessage() {
        EventMessage message = EventMessage.newBuilder().build();
        message.setId("5ba51e3");
        message.setBuilding("building_3");
        message.setMachine("pump_1");
        message.setStatus("Success");
        message.setDate("1234567890L");

        avroProducer.send(message);
    }

}
