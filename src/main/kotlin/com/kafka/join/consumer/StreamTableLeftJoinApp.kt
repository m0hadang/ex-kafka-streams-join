package com.kafka.join.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.kafka.join.Customer
import com.kafka.join.EnrichedOrder
import com.kafka.join.Order
import com.kafka.join.Topics
import com.kafka.join.jsonSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*

fun main() {
    val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
        registerModule(JavaTimeModule())
    }

    val builder = StreamsBuilder()

    // Create KStreams from topics
    // Parse JSON to objects

    val orderSerde = jsonSerde<Order>(objectMapper)
    val customerSerde = jsonSerde<Customer>(objectMapper)

    // Convert customers stream to KTable for join (must specify serdes for state store)
    val customersTable: KTable<String, Customer> = builder
        .stream<String, String>(Topics.CUSTOMERS_TOPIC)
        .mapValues { value ->
            objectMapper.readValue<Customer>(value)
        }
        .selectKey { _, customer -> customer.customerId }
        .groupByKey(Grouped.with(Serdes.String(), customerSerde))
        .reduce({ _, newValue -> newValue }, Materialized.with(Serdes.String(), customerSerde))

    // Join orders with customers
    val enrichedOrders: KStream<String, EnrichedOrder> = builder
        .stream<String, String>(Topics.ORDERS_TOPIC)
        .mapValues { value ->
            objectMapper.readValue<Order>(value)
        }
        .selectKey { _, order -> order.customerId }
        .leftJoin(
            customersTable,
            { order: Order, customer: Customer? ->
                if (customer != null) {
                    println("[consumer] ${customer.customerId}")
                    EnrichedOrder(
                        orderId = order.orderId,
                        customerId = order.customerId,
                        customerTier = customer.tier,
                        productId = order.productId,
                        amount = order.amount,
                        timestamp = order.timestamp
                    )
                } else {
                    println("[consumer] Unknown")
                    EnrichedOrder(
                        orderId = order.orderId,
                        customerId = order.customerId,
                        customerTier = "BRONZE",
                        productId = order.productId,
                        amount = order.amount,
                        timestamp = order.timestamp
                    )
                }
            },
            Joined.with(Serdes.String(), orderSerde, customerSerde)
        )

    // Output enriched orders to a new topic
    enrichedOrders
        .selectKey { _, enriched -> enriched.orderId }
        .mapValues { enriched ->
            objectMapper.writeValueAsString(enriched)
        }
        .to("enriched-orders-topic")

    val streams = KafkaStreams(
        builder.build(),
        Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-join-app")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000)
            put(StreamsConfig.STATE_DIR_CONFIG, "build/kafka-streams-state")
        }
    )

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(
        Thread {
            println("Shutting down streams...")
            streams.close()
        }
    )

    println("Starting Kafka Streams join application...")
    println("Topics:")
    println("  - orders-topic: Input orders stream")
    println("  - customers-topic: Input customers stream")
    println("  - enriched-orders-topic: Output enriched orders")
    println("\nApplication is running. Press Ctrl+C to stop.")

    streams.start()
}
