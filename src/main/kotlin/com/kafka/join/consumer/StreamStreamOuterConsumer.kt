package com.kafka.join.consumer

import com.fasterxml.jackson.module.kotlin.readValue
import com.kafka.join.ConsumerConfig
import com.kafka.join.Customer
import com.kafka.join.ObjectMapperBuilder
import com.kafka.join.Order
import com.kafka.join.OuterJoinResult
import com.kafka.join.Topics
import com.kafka.join.jsonSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.StreamJoined
import java.time.Duration
import kotlin.time.TimeSource

fun main() {
    val objectMapper = ObjectMapperBuilder.build()

    val builder = StreamsBuilder()
    val orderSerde = jsonSerde<Order>(objectMapper)
    val customerSerde = jsonSerde<Customer>(objectMapper)
    val ordersStream: KStream<String, Order> = builder
        .stream<String, String>(Topics.ORDERS_TOPIC)
        .mapValues { value -> objectMapper.readValue<Order>(value) }
        .selectKey { _: String, order: Order -> order.customerId }

    val customersStream: KStream<String, Customer> = builder
        .stream<String, String>(Topics.CUSTOMERS_TOPIC)
        .mapValues { value -> objectMapper.readValue<Customer>(value) }
        .selectKey { _: String, customer: Customer -> customer.customerId }

    val joinWindow = JoinWindows.ofTimeDifferenceAndGrace(
        Duration.ofSeconds(10),
        Duration.ofSeconds(0)
    )

    var lastMark = TimeSource.Monotonic.markNow()
    val outerJoined: KStream<String, OuterJoinResult> = ordersStream.outerJoin(
        customersStream,
        { order: Order?, customer: Customer? ->

            val elapsed = lastMark.elapsedNow()
            lastMark = TimeSource.Monotonic.markNow()

            val result = OuterJoinResult(order = order, customer = customer)
            val kind = when {
                order != null && customer != null -> "BOTH"
                order != null -> "ORDER_ONLY"
                else -> "CUSTOMER_ONLY"
            }
            val timestamp = order?.timestamp ?: customer?.timestamp
            println("[consumer join][${timestamp?.epochSecond}][$elapsed] $kind customerId: ${order?.customerId ?: customer?.customerId}")
            result
        },
        joinWindow,
        StreamJoined.with(Serdes.String(), orderSerde, customerSerde)
    )

    val consumerConfig = ConsumerConfig(name = "customer-orders-stream-stream-outer")

    outerJoined
        .mapValues { result -> objectMapper.writeValueAsString(result) }
        .to(consumerConfig.getTopic())

    val streams = KafkaStreams(
        builder.build(),
        consumerConfig.getProperties()
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Shutting down stream-stream outer join consumer...")
        streams.close()
    })

    println("Starting Kafka Streams stream-stream outer join consumer...")
    println("Output: ${consumerConfig.getTopic()} (order only, customer only, or both)")
    streams.start()
}
