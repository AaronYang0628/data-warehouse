# Use eclipse-temurin JDK 17 as build image
FROM m.daocloud.io/docker.io/library/eclipse-temurin:17-jdk AS builder

# Set working directory
WORKDIR /app

# Copy maven files
COPY pom.xml .
COPY src ./src

# Build the application
RUN ./mvnw clean package -DskipTests

# Create runtime image
FROM m.daocloud.io/docker.io/library/eclipse-temurin:17-jre

WORKDIR /app

# Copy the built artifact from builder stage
COPY --from=builder /app/target/metadata-operator-0.1.0-SNAPSHOT.jar ./app.jar

# Set the entrypoint
ENTRYPOINT ["java", "-jar", "app.jar"]