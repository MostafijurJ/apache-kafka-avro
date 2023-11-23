package com.learn.avro;

import com.learn.avro.producer.AvroProducer;
import com.learn.avro.schema.EventMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class ApacheKafkaAvroApplication {


    public static void main(String[] args) {
        SpringApplication.run(ApacheKafkaAvroApplication.class, args);
    }

}
