package com.kafka.join

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class Order(
    @JsonProperty("orderId") val orderId: String,
    @JsonProperty("customerId") val customerId: String,
    @JsonProperty("productId") val productId: String,
    @JsonProperty("amount") val amount: Double,
    @JsonProperty("timestamp") val timestamp: Instant = Instant.now()
)

data class Customer(
    @JsonProperty("customerId") val customerId: String,
    @JsonProperty("tier") val tier: String, // BRONZE, SILVER, GOLD
    @JsonProperty("timestamp") val timestamp: Instant = Instant.now()
)

data class CustomerOrder(
    @JsonProperty("orderId") val orderId: String,
    @JsonProperty("customerId") val customerId: String,
    @JsonProperty("customerTier") val customerTier: String,
    @JsonProperty("productId") val productId: String,
    @JsonProperty("amount") val amount: Double,
    @JsonProperty("timestamp") val timestamp: Instant
)
