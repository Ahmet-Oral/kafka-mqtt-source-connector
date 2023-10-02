FROM maven:3.9.4-eclipse-temurin-8 AS build
WORKDIR /build
COPY pom.xml ./
RUN mvn dependency:resolve dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests
