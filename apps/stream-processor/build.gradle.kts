plugins {
    kotlin("jvm") version "1.9.0"
    application
}

group = "nl.swapscaps.bonusbox"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
  implementation("io.confluent:kafka-streams-avro-serde:7.4.1")
  implementation("org.apache.kafka:kafka-streams:3.6.0")
  implementation("org.apache.avro:avro:1.11.0")
  implementation(project(":schemas:avro-schemas"))

  testImplementation(kotlin("test"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
  testImplementation("org.testcontainers:junit-jupiter:1.19.1")
  testImplementation("org.testcontainers:kafka:1.19.1")
  testImplementation("org.testcontainers:testcontainers:1.19.1")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}

application {
    mainClass.set("MainKt")
}
