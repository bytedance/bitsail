# Flink 1.11.6 image with hadoop dependencies.
# Online image: blockliu/flink-1.11.6-hadoop-2.7.5

FROM flink:1.11.6

LABEL maintainer="96pengpeng@gmail.com"


ENV HADOOP_UBER_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar

RUN wget -nv -O /opt/flink/lib/flink-shaded-hadoop-2-uber.jar "${HADOOP_UBER_URL}";