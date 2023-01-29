package de.eifinger.kafka_scheduler_client;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaTombstoneProducer {
    void send(ProducerRecord<String, Object> producerRecord);
}
