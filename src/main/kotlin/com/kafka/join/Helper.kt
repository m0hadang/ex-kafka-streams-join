package com.kafka.join

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import java.util.Properties

class ObjectMapperBuilder {
    companion object {
        fun build() = ObjectMapper().apply {
            registerModule(KotlinModule.Builder().build())
            registerModule(JavaTimeModule())
        }
    }
}

class Topics {
    companion object {
        const val ORDERS_TOPIC = "orders-topic"
        const val CUSTOMERS_TOPIC = "customers-topic"
        const val CUSTOMER_ORDERS_TOPIC = "customer-orders-topic"
        const val CUSTOMER_ORDERS_INNER_TOPIC = "customer-orders-inner-topic"
    }
}

class PropertyBuilder {
    companion object {
        fun build(name: String) = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, name)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000)
            put(StreamsConfig.STATE_DIR_CONFIG, "build/$name")
        }
    }
}