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

                // customer is nullable

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
            Joined.with(Serdes.String(), orderSerde, customerSerde)
        )

    val consumerConfig = ConsumerConfig(name = "customer-orders-stream-table-left")

    customerOrders
        .selectKey { _, customer -> customer.orderId }
        .mapValues { customerOrder ->
            objectMapper.writeValueAsString(customerOrder)
        }
        .to(consumerConfig.getTopic())

    val streams = KafkaStreams(
        builder.build(),
        consumerConfig.getProperties(),
    )

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(
        Thread {
            println("Shutting Kafka Streams join consumer...")
            streams.close()
        }
    )

    println("Starting Kafka join consumer...")
    println("Output: ${consumerConfig.getTopic()} (orders+customers within 5min window)")

    streams.start()
}
