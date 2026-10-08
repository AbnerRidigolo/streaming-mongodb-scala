import com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask

plugins {
    java
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "br.com.olist"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
    // Confluent serdes and Schema Registry client are only published here.
    maven("https://packages.confluent.io/maven/")
}

// Kafka 3.5 is the broker shipped by Confluent Platform 7.5 (docker-compose).
val kafkaVersion = "3.5.1"
val confluentVersion = "7.5.0"

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.12.5")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.4")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.4")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// The producers' .avsc files are the single source of truth for the input
// events; the service's own schemas (state, outputs) live in src/main/avro.
val producerSchemas = layout.projectDirectory.dir("../producers/schemas")

tasks.named<GenerateAvroJavaTask>("generateAvroJava") {
    source(producerSchemas)
}

avro {
    isCreateSetters = false
    fieldVisibility = "PRIVATE"
}

application {
    mainClass = "br.com.olist.orderstatus.OrderStatusApp"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
