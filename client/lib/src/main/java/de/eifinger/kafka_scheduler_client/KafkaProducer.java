package de.eifinger.kafka_scheduler_client;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaProducer<V>{
    void send(ProducerRecord<String, V> producerRecord);
}
