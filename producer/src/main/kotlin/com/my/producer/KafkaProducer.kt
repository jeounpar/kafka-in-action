package com.my.producer

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    fun sendMessage(topic: String, key: String, message: String) {
        kafkaTemplate.send(topic, key, message)
            .whenComplete { result, ex ->
                if (ex == null) {
                    println("Message sent - Key: $key, Value: $message, Partition: ${result.recordMetadata.partition()}, Offset: ${result.recordMetadata.offset()}")
                } else {
                    println("Failed to send message: ${ex.message}")
                }
            }
    }
}
