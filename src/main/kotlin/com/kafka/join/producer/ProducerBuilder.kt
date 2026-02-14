package com.kafka.join.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class ProducerBuilder {
    companion object {
        fun buildProducer(): KafkaProducer<String, String> {
            return KafkaProducer<String, String>(
                Properties().apply {
                    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                    put(ProducerConfig.ACKS_CONFIG, "all")
                }
            )
        }
    }
}