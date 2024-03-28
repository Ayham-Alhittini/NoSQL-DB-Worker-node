FROM openjdk:17
ADD target/worker-node.jar worker-node.jar
CMD ["java","-jar","worker-node.jar"]