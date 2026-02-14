package com.kafka.join

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class Order(
    @JsonProperty("orderId") val orderId: String,
    @JsonProperty("customerId") val customerId: String,
    @JsonProperty("productId") val productId: String,
    @JsonProperty("quantity") val quantity: Int,
    @JsonProperty("amount") val amount: Double,
    @JsonProperty("timestamp") val timestamp: Instant = Instant.now()
)

data class Customer(
    @JsonProperty("customerId") val customerId: String,
    @JsonProperty("name") val name: String,
    @JsonProperty("email") val email: String,
    @JsonProperty("tier") val tier: String, // BRONZE, SILVER, GOLD
    @JsonProperty("timestamp") val timestamp: Instant = Instant.now()
)

data class EnrichedOrder(
    @JsonProperty("orderId") val orderId: String,
    @JsonProperty("customerId") val customerId: String,
    @JsonProperty("customerName") val customerName: String,
    @JsonProperty("customerEmail") val customerEmail: String,
    @JsonProperty("customerTier") val customerTier: String,
    @JsonProperty("productId") val productId: String,
    @JsonProperty("quantity") val quantity: Int,
    @JsonProperty("amount") val amount: Double,
    @JsonProperty("timestamp") val timestamp: Instant
)
