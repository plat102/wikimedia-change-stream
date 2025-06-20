plugins {
    id("java")
}

group = "io.data.learn"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.8.1")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.17")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.17")

    // https://mvnrepository.com/artifact/com.squareup.okhttp3/okhttp
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // https://mvnrepository.com/artifact/com.launchdarkly/okhttp-eventsource
//    implementation("com.launchdarkly:okhttp-eventsource:4.1.1")
    implementation("com.launchdarkly:okhttp-eventsource:2.5.0")

    // https://mvnrepository.com/artifact/io.github.cdimascio/java-dotenv
    implementation("io.github.cdimascio:java-dotenv:5.2.2")
}

tasks.test {
    useJUnitPlatform()
}