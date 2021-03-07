package com.ppalwai.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Random;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@Slf4j
public class KafkaDemoApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topicName, String msg) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<String, String>> sendResult =  kafkaTemplate.send(topicName, msg);
        log.info("sendResult.toString(): {}", sendResult.completable().get().getRecordMetadata());
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        while(true) {
            sendMessage("topic1", "Message from Spring Boot: " + new Random().nextInt());
            Thread.sleep(2000);
        }
    }

    @KafkaListener(topics = "topic1", groupId = "group_id")
    public void listen(String message) {
        System.out.println("Received Messasge: " + message);
    }
}