package com.my.kafkalab.config

import com.my.kafkalab.dto.UserEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.JsonDeserializer

@EnableKafka
@Configuration
class KafkaConsumerConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Bean
    fun userEventConsumerFactory(): ConsumerFactory<String, UserEvent> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "kafkalab-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            JsonDeserializer.TRUSTED_PACKAGES to "*"
        )
        return DefaultKafkaConsumerFactory(
            props,
            StringDeserializer(),
            JsonDeserializer(UserEvent::class.java)
        )
    }

    @Bean
    fun userEventKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, UserEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, UserEvent>()
        factory.consumerFactory = userEventConsumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        return factory
    }
}
