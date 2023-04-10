plugins {
   id("software.amazon.smithy").version("0.6.0")
}

buildscript {
    dependencies {

        // This dependency is required in order to apply the "openapi"
        // plugin in smithy-build.json
        classpath("software.amazon.smithy:smithy-openapi:1.29.0")
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("software.amazon.smithy:smithy-model:1.29.0")
    implementation("software.amazon.smithy:smithy-aws-traits:1.29.0")
}

configure<software.amazon.smithy.gradle.SmithyExtension> {
    // Uncomment this to use a custom projection when building the JAR.
    // projection = "foo"
}

// Uncomment to disable creating a JAR.
//tasks["jar"].enabled = false
