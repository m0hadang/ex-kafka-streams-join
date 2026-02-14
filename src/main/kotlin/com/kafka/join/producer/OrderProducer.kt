package com.kafka.join.producer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.kafka.join.Order
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.*
import kotlin.random.Random

fun main() {
    val producer = KafkaProducer<String, String>(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
    )

    var orderCounter = 1
    val customerIds = listOf(
        "CUST001",
        "CUST002",
        "CUST003",
//        "CUST004",
//        "CUST005",
    )
    val productIds = listOf(
        "PROD001",
        "PROD002",
        "PROD003",
    )
    val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
        registerModule(JavaTimeModule())
    }

    println("[order producer] Producing orders to 'orders-topic'...")
    println("[order producer] Press Ctrl+C to stop")

    runCatching {
        while (true) {
            val order = Order(
                orderId = "ORD${String.format("%06d", orderCounter++)}",
                customerId = customerIds.random(),
                productId = productIds.random(),
                quantity = Random.nextInt(1, 10),
                amount = Random.nextDouble(10.0, 500.0),
                timestamp = Instant.now()
            )

            val orderJson = objectMapper.writeValueAsString(order)
            val record = ProducerRecord("orders-topic", order.orderId, orderJson)

            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    println("[order producer] Error sending order: ${exception.message}")
                } else {
                    println("[order producer] Sent order: ${order.orderId} for customer ${order.customerId} -> partition ${metadata.partition()}, offset ${metadata.offset()}")
                }
            }

            Thread.sleep(2000) // Send an order every 2 seconds
        }
    }.onFailure { ex ->
        println("\n[order producer] Stopping order producer...$ex")
    }.also {
        producer.close()
    }
}
