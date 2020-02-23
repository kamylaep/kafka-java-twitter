package com.kep.kafka.producer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventProducer {

  private Logger logger = LoggerFactory.getLogger(KafkaEventProducer.class);
  private String kafkaBootstrapServer = System.getProperty("KAFKA_BOOTSTRAP_SERVER");
  private KafkaProducer<String, String> kafkaProducer;
  private String topic;

  public KafkaEventProducer(String topic) {
    this.topic = topic;
    Properties properties = buildProperties();
    kafkaProducer = new KafkaProducer<>(properties);

    addShutdownHook();
  }

  public void send(String key, String value) {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    kafkaProducer.send(record, this::onCompletionCallback);
    kafkaProducer.flush();
  }

  private void onCompletionCallback(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      logger.error("Error producing message", exception);
    }
  }

  private Properties buildProperties() {
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString());
    properties.setProperty(ACKS_CONFIG, "all");
    properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    return properties;
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down kafka producer");
      kafkaProducer.close();
      logger.info("Done shutting down kafka producer");
    }));
  }
}
