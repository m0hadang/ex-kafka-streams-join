package com.kafka.join.producer

import com.kafka.join.Customer
import com.kafka.join.ObjectMapperBuilder
import com.kafka.join.Topics
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant

fun main() {

    val producer = ProducerBuilder.buildProducer()

    val objectMapper = ObjectMapperBuilder.build()

    val customers = listOf(
        Customer(customerId = "CUST001", tier = "GOLD", timestamp = Instant.now()),
        Customer(customerId = "CUST002", tier = "SILVER", timestamp = Instant.now()),
    )

    println("[customer producer] Producing customer data to 'customers-topic'...")
    println("[customer producer] Press Ctrl+C to stop")

    runCatching {
        customers.forEach { customer ->
            val customerJson = objectMapper.writeValueAsString(customer)
            val record = ProducerRecord(Topics.CUSTOMERS_TOPIC, customer.customerId, customerJson)

            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    println("[customer producer] Error sending customer: ${exception.message}")
                } else {
                    println("[customer producer] Sent customer: ${customer.customerId} -> partition ${metadata.partition()}, offset ${metadata.offset()}")
                }
            }
        }

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
            val record = ProducerRecord(Topics.CUSTOMERS_TOPIC, updatedCustomer.customerId, customerJson)

            producer.send(record) { _metadata, exception ->
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
