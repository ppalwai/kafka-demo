package com.ppalwai.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${topic.name}")
    private String topic;

    public void sendMessage(String topicName, String msg) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<String, String>> sendResult =  kafkaTemplate.send(topicName, msg);
        log.info("sendResult.toString(): {}", sendResult.completable().get().getRecordMetadata());
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        while(true) {
            sendMessage(topic, "Message from Spring Boot: " + new Random().nextInt());
            Thread.sleep(2000);
        }
    }

    @KafkaListener(topics = "first_topic", groupId = "group1")
    public void consumer1_group1(String message) {
        log.info("method: {}, message: {}", "consumer1_group1", message);
    }

    @KafkaListener(topics = "first_topic", groupId = "group1")
    public void consumer2_group1(String message) {
        log.info("method: {}, message: {}", "consumer2_group1", message);
    }

    @KafkaListener(topics = "first_topic", groupId = "group2")
    public void consumer1_group2(String message) {
        log.info("method: {}, message: {}", "consumer1_group2", message);
    }
}