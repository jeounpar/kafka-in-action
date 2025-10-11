package com.my.kafkalab

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkalabApplication

fun main(args: Array<String>) {
    runApplication<KafkalabApplication>(*args)
}
