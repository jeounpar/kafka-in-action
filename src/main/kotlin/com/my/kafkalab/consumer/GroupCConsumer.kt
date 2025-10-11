package com.my.kafkalab.consumer

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class GroupCConsumer {

    @KafkaListener(topics = ["test-topic"], groupId = "group-c-id")
    fun consume(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        acknowledgment: Acknowledgment
    ) {
        println("🟡 [GROUP-C] Received: $message (partition: $partition, offset: $offset)")
        acknowledgment.acknowledge()
    }
}
