plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "com.kafka.join"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:4.1.0")
    implementation("org.apache.kafka:kafka-clients:4.1.0")
    implementation("org.slf4j:slf4j-simple:2.0.9")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.0")
    
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("com.kafka.join.consumer.StreamTableLeftJoinConsumerKt")
}

tasks.register<JavaExec>("runOrderProducer") {
    group = "application"
    description = "Runs the Order Producer"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.kafka.join.producer.OrderProducerKt")
}

tasks.register<JavaExec>("runCustomerProducer") {
    group = "application"
    description = "Runs the Customer Producer"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.kafka.join.producer.CustomerProducerKt")
}

tasks.register<JavaExec>("runInnerJoinConsumer") {
    group = "application"
    description = "Runs the Inner Join Consumer"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.kafka.join.consumer.StreamTableInnerJoinConsumerKt")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}
