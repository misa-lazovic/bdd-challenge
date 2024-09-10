# BDD Challenge

## Structure
Under `src/main/scala/com/bdd/` there is `report/ReportGenerator` that reads the data from CSV, broadcasts it since it's assumed in challenge text that the size will be up to 10MB, then reads the Kafka stream and combines the data to create a report.
The second class there, the `kafka/Producer` is Scala implementation of Kafka producer made to simulate the incoming traffic.
The CSV is stored in `storage` folder.

Folder `test/scala/com/bdd/report/` contains unit test and integration test to test the logic of ReportGenerator as well as integration with Kafka.

Dockerfile and docker-compose define the Docker images and configurations.

Gradle is used as a build tool and dependencies are defined in `build.gradle`, while `settings.gradle` contains only project name. There is also a Gradle wrapper, so the installation of Gradle on the machine is not necessary and the versions used by everyone is guaranteed to be the same.

## Setup
1. First, the project should be built. There is a Gradle wrapper provided for that, so you can only run `./gradlew build` in your Terminal
2. Then Docker can be run using `docker-compose up`
3. Producer.scala can be run independently to simulate the incoming traffic
