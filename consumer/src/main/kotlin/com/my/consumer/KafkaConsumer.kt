package com.my.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Component

@Component
class KafkaConsumer {
    private val logger = LoggerFactory.getLogger(KafkaConsumer::class.java)

    @RetryableTopic(
        attempts = "3",  // 총 3번 시도 (원본 1회 + 재시도 2회)
        backoff = Backoff(delay = 1000),
        dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR,
        dltTopicSuffix = ".dlq"
    )
    @KafkaListener(topics = ["retail-data"], groupId = "consumer-group-a", concurrency = "5")
    fun consumeRetailDataA(record: ConsumerRecord<String, String>) {
        val threadName = Thread.currentThread().name
//        logger.info("[A-$threadName] Consumed - Key: ${record.key()}, Partition: ${record.partition()}, Offset: ${record.offset()}")

        // 테스트용: 특정 조건에서 예외 발생
        if (record.key() == "5") {
            logger.error("[A-$threadName] Failed - Key: ${record.key()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
            throw RuntimeException("Processing failed for key: ${record.key()}")
        }
    }

    @DltHandler
    fun handleDlt(record: ConsumerRecord<String, String>) {
        logger.error("[DLQ] Failed message - Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}")
    }

    // 중복 컨슘
//    @KafkaListener(topics = ["retail-data"], groupId = "consumer-group-b", concurrency = "5")
//    fun consumeRetailDataB(record: ConsumerRecord<String, String>) {
//        val threadName = Thread.currentThread().name
//        logger.info("[B-$threadName] Consumed - Key: ${record.key()}, Partition: ${record.partition()}, Offset: ${record.offset()}")
//    }
}
