package com.my.kafkalab.producer

import com.my.kafkalab.dto.UserEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class UserEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, UserEvent>
) {
    fun sendUserEvent(topic: String, event: UserEvent) {
        kafkaTemplate.send(topic, event.userId, event)
            .whenComplete { result, ex ->
                if (ex == null) {
                    println("UserEvent sent: $event to partition: ${result.recordMetadata.partition()}")
                } else {
                    println("Failed to send UserEvent: ${ex.message}")
                }
            }
    }
}
