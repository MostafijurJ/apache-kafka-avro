package com.learn.avro.controller;

import com.learn.avro.producer.AvroProducer;
import com.learn.avro.schema.EventMessage;
import com.learn.avro.schema.RuleMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
@RequestMapping("/avro")
public class AvroController {

    @Autowired
    private AvroProducer avroProducer;

    @GetMapping("/send")
    void sentMessages() {

        for (int i = 1; i <= 100; i++) {
            EventMessage message = EventMessage.newBuilder()
                    .setId(generateRandomId())
                    .setBuilding(generateRandomBuilding())
                    .setMachine("pump_"+i)
                    .setStatus("Success")
                    .setDate(generateRandomDate())
                    .build();

            avroProducer.sendMessageEvent(message);
        }


        for (int i = 1; i <= 100; i++) {
            RuleMessage ruleMessage = RuleMessage.newBuilder()
                    .setConnection("Connection_"+1)
                    .setModel("pump_" + (i + 1))
                    .setTimestamp(generateRandomDate())
                    .setValue(generateRandomId())  // Adjust the range as needed
                    .setStation(generateRandomStation())
                    .build();

            avroProducer.sendRuleEvent(ruleMessage);
        }
    }

    private String generateRandomId() {
        // Logic to generate a random ID, for example:
        return String.valueOf(new Random().nextInt(1000));
    }

    private String generateRandomBuilding() {
        // Logic to generate a random building name, for example:
        String[] buildings = {"building_1", "building_2", "building_3"};
        return buildings[new Random().nextInt(buildings.length)];
    }

    private String generateRandomStation() {
        // Logic to generate a random station name, for example:
        String[] stations = {"station_1", "station_2", "station_3"};
        return stations[new Random().nextInt(stations.length)];
    }

    private String generateRandomDate() {
        // Logic to generate a random date, for example:
        return String.valueOf(System.currentTimeMillis() - new Random().nextInt(1000000000));
    }


}
