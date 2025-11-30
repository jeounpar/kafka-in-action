package com.my.consumer.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.RetryListener
import org.springframework.util.backoff.FixedBackOff

@EnableKafka
@Configuration
class KafkaConsumerConfig {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Value("\${spring.kafka.bootstrap-servers:localhost:9092}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.consumer.concurrency:6}")
    private var concurrency: Int = 6

    @Value("\${spring.kafka.topic.message.partitions:12}")
    private var messageTopicPartitions: Int = 12

    @Value("\${spring.kafka.topic.message.replicas:3}")
    private var messageTopicReplicas: Int = 3

    // Message 토픽 생성 (파티션 12개)
    @Bean
    fun messageTopic(): NewTopic {
        return TopicBuilder.name("message")
            .partitions(messageTopicPartitions)
            .replicas(messageTopicReplicas)
            .build()
    }

    // DLQ 토픽 생성 (파티션 12개)
    @Bean
    fun messageDlqTopic(): NewTopic {
        return TopicBuilder.name("message.DLT")
            .partitions(messageTopicPartitions)
            .replicas(messageTopicReplicas)
            .build()
    }

    // Producer Factory (DLQ용)
    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to 3
        )
        return DefaultKafkaProducerFactory(props)
    }

    // KafkaTemplate (DLQ용)
    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }

    // Message 토픽용 Consumer Factory
    @Bean
    fun messageConsumerFactory(): ConsumerFactory<String, String> {
        val props = mapOf(
            // 기본 설정
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "message-consumer-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,

            // 오프셋 설정
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",  // 처음부터 읽기
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,       // Spring Kafka ACK 모드 사용

//            // 성능 최적화 (파티션 12개 s대응)
//            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 500,           // 한 번에 가져올 레코드 수
//            ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 1024,           // 최소 1KB 이상 쌓이면 가져오기
//            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 500,          // 최대 0.5초 대기
//            ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG to 1048576, // 파티션당 1MB

            // 세션 관리 (긴 처리 시간 대비)
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to 30000,       // 30초
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to 10000,    // 10초
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300000,    // 5분

            // 파티션 재할당 전략
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG to listOf(
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
            )
        )
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun messageKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = messageConsumerFactory()

        // 동시성 설정 (파티션 12개, Consumer 스레드 6개)
        factory.setConcurrency(concurrency)

        // ACK 모드 설정 - 배치 단위 커밋
        factory.containerProperties.ackMode = ContainerProperties.AckMode.BATCH

        // 에러 핸들러 설정 (재시도 + DLQ)
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate()))

        // 배치 리스너 설정
        factory.isBatchListener = false  // 메시지별 처리

        return factory
    }

    // 에러 핸들러 (재시도 + DLQ)
    @Bean
    fun errorHandler(kafkaTemplate: KafkaTemplate<String, String>): CommonErrorHandler {
        // DLQ로 보내는 Recoverer 설정
        val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate) { record, _ ->
            // DLQ 토픽 이름: 원본토픽.DLT
            org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_TOPIC
            org.apache.kafka.common.TopicPartition("${record.topic()}.dlq", record.partition())
        }

        val errorHandler = DefaultErrorHandler(
            recoverer,
            // 재시도 없이 바로 DLQ로 전송
            FixedBackOff(0L, 0L)  // interval, maxAttempts (0 = 재시도 없음)
        )

        // 재시도하지 않을 예외 등록 (선택사항)
        // errorHandler.addNotRetryableExceptions(IllegalArgumentException::class.java)

        // 에러 로깅 - RetryListener 설정
        errorHandler.setRetryListeners(object : RetryListener {
            override fun failedDelivery(
                record: ConsumerRecord<*, *>,
                ex: Exception,
                deliveryAttempt: Int
            ) {
                logger.warn(
                    "Retry attempt $deliveryAttempt for record: " +
                    "topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}",
                    ex
                )
            }

            override fun recovered(
                record: ConsumerRecord<*, *>,
                ex: Exception
            ) {
                logger.error(
                    "All retries exhausted. Sending to DLQ: " +
                    "topic=${record.topic()}, partition=${record.partition()}, offset=${record.offset()}",
                    ex
                )
            }
        })

        return errorHandler
    }
}
