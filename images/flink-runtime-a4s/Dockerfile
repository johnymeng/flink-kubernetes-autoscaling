ARG MAVEN_IMAGE=maven:3.8.8-eclipse-temurin-11@sha256:25d160c54fe5249795ffe4c4ab10fb0b305c746a7721d699b28d48de3e1347fd
ARG FLINK_IMAGE=ghcr.io/apache/flink-docker:1.18-SNAPSHOT-scala_2.12-java11-debian@sha256:38d4ccb5891801cf04d721911b9645cbbf818804f0b53bcbee7e19bfb64d9346

FROM ${MAVEN_IMAGE} AS build
ARG SKIP_TESTS=true
ARG HTTP_CLIENT=okhttp
ARG FLINK_RUNTIME_SOURCE_DIR=sources/flink/flink-runtime-1-18-a4s
ARG ROCKSDBJNI_JAR=rocksdbjni-6.20.3-linux64.jar

WORKDIR /app

COPY ${FLINK_RUNTIME_SOURCE_DIR}/ .

RUN mvn -B -ntp install:install-file \
    -Dfile=${ROCKSDBJNI_JAR} \
    -DgroupId=org.rocksdb \
    -DartifactId=frocksdbjni \
    -Dversion=6.20.3-custom \
    -Dpackaging=jar
RUN mvn -B -ntp clean install -T 4 -DskipTests -Dspotless.check.skip=true -Drat.skip=true -Dcheckstyle.skip

FROM ${FLINK_IMAGE}

RUN rm -rf /opt/flink
USER flink
COPY --from=build --chown=flink:flink /app/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/ /opt/flink
RUN mkdir /opt/flink/plugins/flink-s3-fs-hadoop; \
cp /opt/flink/opt/flink-s3-fs-hadoop-1.18-SNAPSHOT.jar /opt/flink/plugins/flink-s3-fs-hadoop/
ENV FLINK_HOME=/opt/flink

RUN sed -i 's/rest.address: localhost/rest.address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i 's/rest.bind-address: localhost/rest.bind-address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i 's/jobmanager.bind-host: localhost/jobmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i 's/taskmanager.bind-host: localhost/taskmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i '/taskmanager.host: localhost/d' $FLINK_HOME/conf/flink-conf.yaml;
