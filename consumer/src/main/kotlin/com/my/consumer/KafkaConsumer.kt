package com.my.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class KafkaConsumer {
    private val logger = LoggerFactory.getLogger(KafkaConsumer::class.java)

    @KafkaListener(topics = ["retail-data"], groupId = "consumer-group-a", concurrency = "5")
    fun consumeRetailDataA(record: ConsumerRecord<String, String>) {
        val threadName = Thread.currentThread().name
        logger.info("[A-$threadName] Consumed - Key: ${record.key()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
    }

    // 중복 컨슘
    @KafkaListener(topics = ["retail-data"], groupId = "consumer-group-b", concurrency = "5")
    fun consumeRetailDataB(record: ConsumerRecord<String, String>) {
        val threadName = Thread.currentThread().name
        logger.info("[B-$threadName] Consumed - Key: ${record.key()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
    }
}
