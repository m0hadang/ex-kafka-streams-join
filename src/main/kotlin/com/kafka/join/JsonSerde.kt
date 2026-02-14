package com.kafka.join

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

inline fun <reified T : Any> jsonSerde(objectMapper: ObjectMapper): Serde<T> {
    return object : Serde<T> {
        override fun serializer(): Serializer<T> = JsonSerializer(objectMapper)
        override fun deserializer(): Deserializer<T> = JsonDeserializer(objectMapper, T::class.java)
    }
}

class JsonSerializer<T>(private val objectMapper: ObjectMapper) : Serializer<T> {
    override fun serialize(topic: String?, data: T?): ByteArray? {
        if (data == null) return null
        return objectMapper.writeValueAsBytes(data)
    }
}

class JsonDeserializer<T : Any>(
    private val objectMapper: ObjectMapper,
    private val clazz: Class<T>
) : Deserializer<T> {
    override fun deserialize(topic: String?, data: ByteArray?): T? {
        if (data == null) return null
        return objectMapper.readValue(data, clazz)
    }
}
