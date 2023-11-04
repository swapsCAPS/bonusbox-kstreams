pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
        maven(url = "https://packages.confluent.io/maven/")
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}

rootProject.name = "bonusbox"
include("apps:producer")
include("apps:stream-processor")
include("schemas:avro-schemas")
