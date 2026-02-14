package com.kafka.join.consumer

import com.fasterxml.jackson.module.kotlin.readValue
import com.kafka.join.ConsumerConfig
import com.kafka.join.Customer
import com.kafka.join.CustomerOrder
import com.kafka.join.ObjectMapperBuilder
import com.kafka.join.Order
import com.kafka.join.Topics
import com.kafka.join.jsonSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.StreamJoined
import java.time.Duration

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
        Duration.ofSeconds(5)
    )

    // KStream-KStream left join: emits for every order; customer is null when no match in window
    val customerOrders: KStream<String, CustomerOrder> = ordersStream.leftJoin(
        customersStream,
        { order: Order, customer: Customer? ->
            val co = CustomerOrder(
                orderId = order.orderId,
                customerId = order.customerId,
                customerTier = customer?.tier ?: "Unknown",
                productId = order.productId,
                amount = order.amount,
                timestamp = order.timestamp
            )
            println("[consumer join] customerId: ${co.customerId}, orderId: ${co.orderId}, tier: ${co.customerTier}")
            co
        },
        joinWindow,
        StreamJoined.with(Serdes.String(), orderSerde, customerSerde)
    )

    val consumerConfig = ConsumerConfig(name = "customer-orders-stream-stream-left")

    customerOrders
        .selectKey { _, co -> co.orderId }
        .mapValues { co -> objectMapper.writeValueAsString(co) }
        .to(consumerConfig.getTopic())

    val streams = KafkaStreams(
        builder.build(),
        consumerConfig.getProperties()
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Shutting Kafka Streams join consumer...")
        streams.close()
    })

    println("Starting Kafka join consumer...")
    println("Output: ${consumerConfig.getTopic()} (all orders; tier=Unknown when no customer in window)")
    streams.start()
}
