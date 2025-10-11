package com.my.kafkalab.dto

data class UserEvent(
    val userId: String,
    val action: String,
    val timestamp: Long = System.currentTimeMillis()
)
