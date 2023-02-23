FROM flink:1.11.6-scala_2.11-java8
ENV USERLIB=/opt/bitsail
ENV HADOOP_UBER_URL=https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-shaded-hadoop-3/3.1.1.7.2.8.0-224-9.0/flink-shaded-hadoop-3-3.1.1.7.2.8.0-224-9.0.jar
RUN mkdir -p $USERLIB
COPY libs/ $USERLIB/
RUN wget -nv -O /opt/flink/lib/flink-shaded-hadoop-3-uber.jar "${HADOOP_UBER_URL}";