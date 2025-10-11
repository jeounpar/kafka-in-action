package com.my.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class KafkaConsumer {

    @KafkaListener(topics = ["user-events"], groupId = "consumer-group")
    fun consumeUserEventA(record: ConsumerRecord<String, String>) {
        println("[A] Consumed message - Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
    }

    @KafkaListener(topics = ["user-events"], groupId = "consumer-group")
    fun consumeUserEventB(record: ConsumerRecord<String, String>) {
        println("[B] Consumed message - Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
    }

    @KafkaListener(topics = ["user-events"], groupId = "consumer-group")
    fun consumeUserEventC(record: ConsumerRecord<String, String>) {
        println("[C] Consumed message - Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
    }
}
