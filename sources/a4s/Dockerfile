FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY . .
RUN mvn -B -ntp package

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /build/a4s-runner/target/a4s-runner.jar /app/a4s-runner.jar
ENV A4S_BIND=0.0.0.0
EXPOSE 8080
USER 10001:10001
ENTRYPOINT ["java", "-jar", "/app/a4s-runner.jar"]
