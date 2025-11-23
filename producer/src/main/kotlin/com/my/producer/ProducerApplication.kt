package com.my.producer

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.core.io.ClassPathResource

@SpringBootApplication
class ProducerApplication {

    @Bean
    fun init(kafkaProducer: KafkaProducer): CommandLineRunner = CommandLineRunner {
        val resource = ClassPathResource("e-commerce-retail-data.csv")
        resource.inputStream.bufferedReader().useLines { lines ->
            lines.drop(1).forEachIndexed { index, line ->
                kafkaProducer.sendMessage("retail-data", index.toString(), line)
            }
        }
    }
}

fun main(args: Array<String>) {
    runApplication<ProducerApplication>(*args)
}
