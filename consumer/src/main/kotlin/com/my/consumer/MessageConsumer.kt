package com.my.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class MessageConsumer {
    private val logger = LoggerFactory.getLogger(MessageConsumer::class.java)

    @KafkaListener(
        topics = ["message"],
        groupId = "message-consumer-group",
        containerFactory = "messageKafkaListenerContainerFactory"
    )
    fun consumeMessage(record: ConsumerRecord<String, String>) {
        logger.info(
            "Key: ${record.key()}, " +
            "Partition: ${record.partition()}, Offset: ${record.offset()}, " +
            "Value: ${record.value()}"
        )

        try {
            // 메시지 처리 로직
            processMessage(record)
            // 정상 처리 시 ACK는 배치 단위 자동 커밋 (BATCH 모드)
        } catch (e: Exception) {
            logger.error(
                "Failed to process message: " +
                "Key=${record.key()}, Partition=${record.partition()}, Offset=${record.offset()}",
                e
            )
            // 예외를 다시 던져서 커밋 방지 및 DefaultErrorHandler(재시도 + DLQ) 작동
            throw e
        }
    }

    private fun processMessage(record: ConsumerRecord<String, String>) {
        throw RuntimeException()
        // 실제 비즈니스 로직 구현
        // 예: 데이터베이스 저장, 외부 API 호출 등

        // 테스트용: 특정 키에서 에러 발생 시 DLQ로 전송
        // if (record.key() == "error") {
        //     throw RuntimeException("Test error for DLQ")
        // }
    }

    @KafkaListener(
        topics = ["message.dlq"],
        groupId = "message-dlq-consumer-group",
        containerFactory = "messageKafkaListenerContainerFactory"
    )
    fun consumeDlqMessage(record: ConsumerRecord<String, String>) {
        logger.error(
            "[DLQ] Failed message received: " +
            "Key: ${record.key()}, " +
            "Partition: ${record.partition()}, " +
            "Offset: ${record.offset()}, " +
            "Value: ${record.value()}"
        )

        // DLQ 메시지 처리 로직
        // 예: 에러 로그 DB 저장, 알림 발송, 수동 재처리 대기 등
        handleDlqMessage(record)
    }

    private fun handleDlqMessage(record: ConsumerRecord<String, String>) {
        // DLQ 메시지 처리 로직 구현
        logger.info("[DLQ] Message logged for manual review")
    }
}
