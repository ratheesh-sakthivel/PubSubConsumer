FROM openjdk:8-jdk-alpine

WORKDIR /com/ratheesh

ADD target/pubsubconsumer-1.0-SNAPSHOT.jar /com/ratheesh
EXPOSE 8080
CMD ["java", "-jar", "pubsubconsumer-1.0-SNAPSHOT.jar"]