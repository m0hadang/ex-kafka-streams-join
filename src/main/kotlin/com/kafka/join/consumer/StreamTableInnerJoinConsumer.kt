package com.kafka.join.consumer

import com.fasterxml.jackson.module.kotlin.readValue
import com.kafka.join.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*

fun main() {
    val objectMapper = ObjectMapperBuilder.build()

    val builder = StreamsBuilder()
    val orderSerde = jsonSerde<Order>(objectMapper)
    val customerSerde = jsonSerde<Customer>(objectMapper)

    val customersTable: KTable<String, Customer> = builder
        .stream<String, String>(Topics.CUSTOMERS_TOPIC)
        .mapValues { value -> objectMapper.readValue<Customer>(value) }
        .selectKey { _, customer -> customer.customerId }
        .groupByKey(Grouped.with(Serdes.String(), customerSerde))
        .reduce({ _, newValue -> newValue }, Materialized.with(Serdes.String(), customerSerde))

    // Inner join: emits ONLY when both order and customer match (no null customer)
    val customerOrders: KStream<String, CustomerOrder> = builder
        .stream<String, String>(Topics.ORDERS_TOPIC)
        .mapValues { value -> objectMapper.readValue<Order>(value) }
        .selectKey { _, order -> order.customerId }
        .join(
            customersTable,
            { order: Order, customer: Customer ->
                val customerOrder = CustomerOrder(
                    orderId = order.orderId,
                    customerId = order.customerId,
                    customerTier = customer.tier,
                    productId = order.productId,
                    amount = order.amount,
                    timestamp = order.timestamp
                )
                println("[inner join] customerId: ${customerOrder.customerId}, tier: ${customerOrder.customerTier}")
                customerOrder
            },
            Joined.with(Serdes.String(), orderSerde, customerSerde)
        )

    customerOrders
        .selectKey { _, co -> co.orderId }
        .mapValues { co -> objectMapper.writeValueAsString(co) }
        .to(Topics.CUSTOMER_ORDERS_INNER_TOPIC)

    val streams = KafkaStreams(
        builder.build(),
        Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-inner-join-app")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000)
            put(StreamsConfig.STATE_DIR_CONFIG, "build/kafka-streams-state-inner")
        }
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Shutting down inner join consumer...")
        streams.close()
    })

    println("Starting Kafka Streams inner join consumer...")
    println("Output: ${Topics.CUSTOMER_ORDERS_INNER_TOPIC} (only orders with matching customer)")
    streams.start()
}
