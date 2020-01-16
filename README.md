# Acquisition Microservice

This is the Java implementation of the Acquisition Microservice of [Immunizer: The Collaborative Cloud-based Unsupervised Software Immunity Framework](https://github.com/oiraqi/immunizer). It leverages distributed and parallel processing thanks to cluster-computing frameworks such as Apache Spark and Google DataFlow, abstracted and unified through Apache Beam.

## Siblings
- [Monitoring Microagent](https://github.com/oiraqi/immunizer-monitoring)
- [Analysis Microservice](https://github.com/oiraqi/immunizer-analysis)
- [Collaboration Microservice](https://github.com/oiraqi/immunizer-collaboration)
- [Response Microagent](https://github.com/oiraqi/immunizer-response)

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
