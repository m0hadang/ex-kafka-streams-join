package com.kafka.join.producer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.kafka.join.Customer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.*

fun main() {

    val producer = KafkaProducer<String, String>(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
    )

    val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
        registerModule(JavaTimeModule())
    }

    val customers = listOf(
        Customer("CUST001", "Alice Johnson", "alice@example.com", "GOLD", Instant.now()),
        Customer("CUST002", "Bob Smith", "bob@example.com", "SILVER", Instant.now()),
//        Customer("CUST003", "Charlie Brown", "charlie@example.com", "BRONZE", Instant.now()),
//        Customer("CUST004", "Diana Prince", "diana@example.com", "GOLD", Instant.now()),
//        Customer("CUST005", "Eve Wilson", "eve@example.com", "SILVER", Instant.now())
    )

    println("[customer producer] Producing customer data to 'customers-topic'...")
    println("[customer producer] Press Ctrl+C to stop")

    runCatching {
        // Send initial customer data
        customers.forEach { customer ->
            val customerJson = objectMapper.writeValueAsString(customer)
            val record = ProducerRecord("customers-topic", customer.customerId, customerJson)

            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    println("[customer producer] Error sending customer: ${exception.message}")
                } else {
                    println("[customer producer] Sent customer: ${customer.customerId} (${customer.name}) -> partition ${metadata.partition()}, offset ${metadata.offset()}")
                }
            }
        }

        // Wait a bit, then send updates periodically
        Thread.sleep(5000)

        var updateCounter = 0
        while (true) {
            // Occasionally update customer tier
            val customer = customers.random()
            val updatedCustomer = customer.copy(
                tier = when (customer.tier) {
                    "BRONZE" -> "SILVER"
                    "SILVER" -> "GOLD"
                    else -> "BRONZE"
                },
                timestamp = Instant.now()
            )

            val customerJson = objectMapper.writeValueAsString(updatedCustomer)
            val record = ProducerRecord("customers-topic", updatedCustomer.customerId, customerJson)

            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    println("[customer producer] Error sending customer update: ${exception.message}")
                } else {
                    println("[customer producer] Updated customer: ${updatedCustomer.customerId} -> tier ${updatedCustomer.tier} (update #${++updateCounter})")
                }
            }

            Thread.sleep(10000) // Send an update every 10 seconds
        }
    }.onFailure { ex ->
        println("\n[customer producer] Stopping customer producer... $ex")
    }.also {
        producer.close()
    }
}
