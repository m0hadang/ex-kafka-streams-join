package com.kafka.join.producer

import com.kafka.join.ObjectMapperBuilder
import com.kafka.join.Order
import com.kafka.join.Topics
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant
import kotlin.random.Random

fun main() {
    val objectMapper = ObjectMapperBuilder.build()

    val producer = ProducerBuilder.buildProducer()

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

    println("[order producer] Producing orders to 'orders-topic'...")
    println("[order producer] Press Ctrl+C to stop")

    runCatching {
        while (true) {
            val order = Order(
                orderId = "ORD${String.format("%06d", orderCounter++)}",
                customerId = customerIds.random(),
                productId = productIds.random(),
                amount = Random.nextDouble(10.0, 500.0),
                timestamp = Instant.now()
            )

            val orderJson = objectMapper.writeValueAsString(order)
            val record = ProducerRecord(Topics.ORDERS_TOPIC, order.orderId, orderJson)

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
