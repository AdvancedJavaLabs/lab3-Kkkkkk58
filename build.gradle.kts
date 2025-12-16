plugins {
    java
    application
}

group = "ru.itmo.parallel"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.hadoop:hadoop-common:3.2.1")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:3.2.1")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-jobclient:3.2.1")
    implementation("org.slf4j:slf4j-simple:2.0.16")

    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")
}

application {
    mainClass = "ru.itmo.parallel.mapreduce.SalesApp"
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "ru.itmo.parallel.mapreduce.SalesApp"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }) {
        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    }
}

