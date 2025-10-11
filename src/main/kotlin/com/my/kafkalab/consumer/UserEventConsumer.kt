package com.my.kafkalab.consumer

import com.my.kafkalab.dto.UserEvent
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class UserEventConsumer {

    @KafkaListener(
        topics = ["user-events"],
        groupId = "kafkalab-group",
        containerFactory = "userEventKafkaListenerContainerFactory"
    )
    fun consumeUserEvent(
        @Payload event: UserEvent,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        acknowledgment: Acknowledgment
    ) {
        println("=== UserEvent Received ===")
        println("Topic: $topic")
        println("Partition: $partition")
        println("Offset: $offset")
        println("UserId: ${event.userId}")
        println("Action: ${event.action}")
        println("Timestamp: ${event.timestamp}")
        println("==========================")

        // 수동 커밋
        acknowledgment.acknowledge()
    }
}
