# Acquisition Microservice of Immunizer

This is the Java implementation of the acquisition microservice of [Immunizer: The Collaborative Cloud-based Unsupervised Software Immunity Framework](https://github.com/oiraqi/immunizer)

## Siblings
- [Instrumentation Microservice of Immunizer](https://github.com/oiraqi/immunizer-instrumentation)
- [Analysis Microservice of Immunizer](https://github.com/oiraqi/immunizer-analysis)
- [Collaboration Microservice of Immunizer](https://github.com/oiraqi/immunizer-collaboration)

## Dependencies

All dependencies are managed through Gradle.

## Structure
- framework: source code and dependencies managed by Gradle

## Current Environment
- Linux Ubuntu 18.04 (Bionic)
- OpenJDK 8
- Gson 2.8.6
- Apache Kafka Clients API 2.4.0

## How To
- Clone this repository
- cd framework
- ./gradlew libs
- cd build/libs
- java -cp libs-1.0.jar org.immunizer.acquisition.AcquisitionApplication
