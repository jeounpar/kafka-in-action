package com.my.producer

import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/messages")
class ProducerController(
    private val kafkaProducer: KafkaProducer
) {

    @PostMapping("/send")
    fun sendMessage(
        @RequestParam topic: String,
        @RequestParam key: String,
        @RequestParam message: String
    ): Map<String, String> {
        kafkaProducer.sendMessage(topic, key, message)
        return mapOf(
            "status" to "success",
            "topic" to topic,
            "key" to key,
            "message" to message
        )
    }

    @PostMapping("/send-batch")
    fun sendBatchMessages(
        @RequestParam topic: String,
        @RequestParam count: Int = 10
    ): Map<String, Any> {
        repeat(count) { i ->
            kafkaProducer.sendMessage(topic, "key-$i", "message-$i")
        }
        return mapOf(
            "status" to "success",
            "topic" to topic,
            "count" to count
        )
    }
}
