# syntax=docker/dockerfile:1.7

FROM gradle:9.3.1-jdk17 AS build
WORKDIR /workspace

COPY gradle gradle
COPY gradlew gradlew
COPY settings.gradle build.gradle ./

# Normalize line endings for Linux container builds created from Windows hosts.
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew

COPY src src

RUN --mount=type=cache,target=/home/gradle/.gradle \
    ./gradlew --no-daemon clean bootJar -x test

FROM eclipse-temurin:17-jre-jammy AS runtime
WORKDIR /app

RUN groupadd --system spring && useradd --system --gid spring spring

COPY --from=build /workspace/build/libs/*.jar /app/app.jar

USER spring:spring
EXPOSE 8080

# Cloud Run injects PORT; default to 8080 for local runs.
ENV PORT=8080
ENV JAVA_OPTS=""

# Use exec so the JVM gets termination signals directly.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Dserver.port=${PORT} -jar /app/app.jar"]
