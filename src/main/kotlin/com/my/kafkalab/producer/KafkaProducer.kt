package com.my.kafkalab.producer

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    fun sendMessage(topic: String, message: String) {
        kafkaTemplate.send(topic, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    println("Message sent successfully: $message to topic: $topic, partition: ${result.recordMetadata.partition()}, offset: ${result.recordMetadata.offset()}")
                } else {
                    println("Failed to send message: ${ex.message}")
                }
            }
    }

    fun sendMessage(topic: String, key: String, message: String) {
        kafkaTemplate.send(topic, key, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    println("Message sent successfully with key: $key, message: $message to topic: $topic")
                } else {
                    println("Failed to send message: ${ex.message}")
                }
            }
    }
}
