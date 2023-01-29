package de.eifinger.kafka.scheduler.client;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaProducer<K, V> {

    void send(ProducerRecord<K, V> producerRecord);
}
