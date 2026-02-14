package com.kafka.join.consumer

import com.fasterxml.jackson.module.kotlin.readValue
import com.kafka.join.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*

fun main() {
    val objectMapper = ObjectMapperBuilder.build()

    val builder = StreamsBuilder()

    val orderSerde = jsonSerde<Order>(objectMapper)
    val customerSerde = jsonSerde<Customer>(objectMapper)

    val customersTable: KTable<String, Customer> = builder
        .stream<String, String>(Topics.CUSTOMERS_TOPIC)
        .mapValues { value ->
            objectMapper.readValue<Customer>(value)
        }
        .selectKey { _, customer -> customer.customerId }
        .groupByKey(Grouped.with(Serdes.String(), customerSerde))
        .reduce({ _, newValue -> newValue }, Materialized.with(Serdes.String(), customerSerde))

    val customerOrders: KStream<String, CustomerOrder> = builder
        .stream<String, String>(Topics.ORDERS_TOPIC)
        .mapValues { value ->
            objectMapper.readValue<Order>(value)
        }
        .selectKey { _, order -> order.customerId }
        .leftJoin(
            customersTable,
            { order: Order, customer: Customer? ->

                val customerOrder = customer?.let {
                    CustomerOrder(
                        orderId = order.orderId,
                        customerId = order.customerId,
                        customerTier = customer.tier,
                        productId = order.productId,
                        amount = order.amount,
                        timestamp = order.timestamp
                    )
                } ?: CustomerOrder(
                    orderId = order.orderId,
                    customerId = order.customerId,
                    customerTier = "Unknown",
                    productId = order.productId,
                    amount = order.amount,
                    timestamp = order.timestamp
                )

                println("[customer order consumer] customerId: ${customerOrder.customerId}, customerTier: ${customerOrder.customerTier}")

                customerOrder
            },
            Joined.with(Serdes.String(), orderSerde, customerSerde)
        )

    customerOrders
        .selectKey { _, customer -> customer.orderId }
        .mapValues { customerOrder ->
            objectMapper.writeValueAsString(customerOrder)
        }
        .to(Topics.CUSTOMER_ORDERS_TOPIC)

    val streams = KafkaStreams(
        builder.build(),
        PropertyBuilder.build("kafka-state-stream-table-left"),
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
    println("  - customer-orders-topic: Output customer orders")
    println("\nApplication is running. Press Ctrl+C to stop.")

    streams.start()
}
