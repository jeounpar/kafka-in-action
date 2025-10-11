package com.my.kafkalab.controller

import com.my.kafkalab.dto.UserEvent
import com.my.kafkalab.producer.KafkaProducer
import com.my.kafkalab.producer.UserEventProducer
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/kafka")
class KafkaTestController(
    private val kafkaProducer: KafkaProducer,
    private val userEventProducer: UserEventProducer
) {

    @GetMapping("/send")
    fun sendMessage(
        @RequestParam(defaultValue = "test-topic") topic: String,
        @RequestParam(defaultValue = "Hello Kafka!") message: String
    ): String {
        kafkaProducer.sendMessage(topic, message)
        return "Message sent: $message to topic: $topic"
    }

    @GetMapping("/send-with-key")
    fun sendMessageWithKey(
        @RequestParam(defaultValue = "test-topic") topic: String,
        @RequestParam key: String,
        @RequestParam message: String
    ): String {
        kafkaProducer.sendMessage(topic, key, message)
        return "Message sent with key: $key, message: $message to topic: $topic"
    }

    @PostMapping("/send-user-event")
    fun sendUserEvent(
        @RequestBody event: UserEvent
    ): String {
        userEventProducer.sendUserEvent("user-events", event)
        return "UserEvent sent: $event"
    }

    @GetMapping("/send-user-event")
    fun sendUserEventGet(
        @RequestParam userId: String,
        @RequestParam action: String
    ): String {
        val event = UserEvent(userId, action)
        userEventProducer.sendUserEvent("user-events", event)
        return "UserEvent sent: $event"
    }
}
